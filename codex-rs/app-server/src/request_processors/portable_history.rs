//! Explicit local portable-history extension. Upstream peers without negotiated support are untouched.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use codex_app_server_protocol::PORTABLE_HISTORY_ROLLOUT_SCHEMA;
use codex_app_server_protocol::PORTABLE_HISTORY_VERSION;
use codex_app_server_protocol::ThreadPortableHistoryExportParams;
use codex_app_server_protocol::ThreadPortableHistoryExportResponse;
use codex_app_server_protocol::ThreadPortableHistoryImportParams;
use codex_app_server_protocol::ThreadPortableHistoryReleaseParams;
use codex_app_server_protocol::ThreadPortableHistoryReleaseResponse;
use codex_protocol::protocol::AgentStatus;
use codex_thread_store::portable_history::PortableHistoryArtifact;
use tokio::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use super::*;

const EXPORT_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_EXPORTS: usize = 8;

struct ExportLease {
    connection_id: ConnectionId,
    created_at: Instant,
    _permit: OwnedSemaphorePermit,
    _artifact: PortableHistoryArtifact,
}

#[derive(Default)]
struct ExportState {
    live_connections: HashSet<ConnectionId>,
    exports: HashMap<String, ExportLease>,
}

pub(super) struct PortableHistoryExports {
    state: Mutex<ExportState>,
    slots: Arc<Semaphore>,
    imports: Arc<Semaphore>,
}

impl Default for PortableHistoryExports {
    fn default() -> Self {
        Self {
            state: Mutex::new(ExportState::default()),
            slots: Arc::new(Semaphore::new(MAX_EXPORTS)),
            imports: Arc::new(Semaphore::new(1)),
        }
    }
}

impl PortableHistoryExports {
    pub(super) async fn connection_initialized(&self, connection_id: ConnectionId) {
        self.state.lock().await.live_connections.insert(connection_id);
    }

    pub(super) async fn connection_closed(&self, connection_id: ConnectionId) {
        let mut state = self.state.lock().await;
        state.live_connections.remove(&connection_id);
        state.exports.retain(|_, lease| lease.connection_id != connection_id);
    }

    async fn reserve(&self, connection_id: ConnectionId) -> Result<OwnedSemaphorePermit, JSONRPCErrorError> {
        let mut state = self.state.lock().await;
        state.exports.retain(|_, lease| lease.created_at.elapsed() < EXPORT_TTL);
        if !state.live_connections.contains(&connection_id) {
            return Err(invalid_request("portable history connection is closed"));
        }
        Arc::clone(&self.slots).try_acquire_owned()
            .map_err(|_| invalid_request("portable history export capacity is full; release an existing export"))
    }
}

fn validate_schema(version: u32, schema: &str) -> Result<(), JSONRPCErrorError> {
    if version != PORTABLE_HISTORY_VERSION || schema != PORTABLE_HISTORY_ROLLOUT_SCHEMA {
        return Err(invalid_request("unsupported portable history schema"));
    }
    Ok(())
}

impl ThreadRequestProcessor {
    pub(crate) async fn thread_portable_history_export(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadPortableHistoryExportParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        validate_schema(params.version, &params.rollout_schema)?;
        if !self.thread_store.supports_portable_history() {
            return Err(method_not_found("portable history is unavailable for this store"));
        }
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|_| invalid_request("invalid portable history source thread id"))?;
        // A registered turnless root retains PendingInit until its first turn.
        // Like thread/turns/list, reject Running; the store's terminal-history
        // validation independently rejects unfinished persisted input.
        if let Ok(thread) = self.thread_manager.get_thread(thread_id).await
            && matches!(thread.agent_status().await, AgentStatus::Running)
        {
            return Err(invalid_request("portable history requires an idle source conversation"));
        }
        let permit = self.portable_history_exports.reserve(request_id.connection_id).await?;
        let artifact = self.thread_store.export_portable_history(thread_id).await
            .map_err(|error| match error {
                ThreadStoreError::Unsupported { operation } => method_not_found(format!("unsupported portable history: {operation}")),
                ThreadStoreError::InvalidRequest { message } | ThreadStoreError::Conflict { message } => invalid_request(message),
                error => internal_error(format!("failed to export portable history: {error}")),
            })?;
        let export_id = uuid::Uuid::now_v7().to_string();
        let response = ThreadPortableHistoryExportResponse {
            version: PORTABLE_HISTORY_VERSION,
            rollout_schema: PORTABLE_HISTORY_ROLLOUT_SCHEMA.to_string(),
            export_id: export_id.clone(), source_thread_id: thread_id.to_string(),
            path: AbsolutePathBuf::from_absolute_path(artifact.path()).map_err(|error| internal_error(error.to_string()))?,
            byte_count: artifact.byte_count, sha256: artifact.sha256.clone(),
            record_count: artifact.record_count, terminal_turn_id: artifact.terminal_turn_id.clone(),
        };
        {
            let mut state = self.portable_history_exports.state.lock().await;
            if !state.live_connections.contains(&request_id.connection_id) {
                return Err(invalid_request("portable history connection closed before export publication"));
            }
            state.exports.insert(export_id, ExportLease {
                connection_id: request_id.connection_id, created_at: Instant::now(),
                _permit: permit, _artifact: artifact,
            });
        }
        Ok(Some(response.into()))
    }

    pub(crate) async fn thread_portable_history_release(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadPortableHistoryReleaseParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let mut state = self.portable_history_exports.state.lock().await;
        state.exports.retain(|_, lease| lease.created_at.elapsed() < EXPORT_TTL);
        if state.exports.get(&params.export_id)
            .is_some_and(|lease| lease.connection_id != request_id.connection_id)
        {
            return Err(invalid_request("portable history export belongs to another connection"));
        }
        state.exports.remove(&params.export_id);
        Ok(Some(ThreadPortableHistoryReleaseResponse {}.into()))
    }

    pub(crate) async fn thread_portable_history_import(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadPortableHistoryImportParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        validate_schema(params.version, &params.rollout_schema)?;
        if !self.thread_store.supports_portable_history() {
            return Err(method_not_found("portable history is unavailable for this store"));
        }
        let source_thread_id = ThreadId::from_string(&params.source_thread_id)
            .map_err(|_| invalid_request("invalid portable history source thread id"))?;
        let fork = &params.fork;
        if fork.thread_id != params.source_thread_id || fork.path.is_some()
            || fork.last_turn_id.is_some() || fork.before_turn_id.is_some()
            || fork.experimental_prediction_mode || fork.ephemeral || fork.defer_goal_continuation
            || !fork.exclude_turns
            || !fork.cwd.as_deref().is_some_and(|cwd| !cwd.is_empty() && std::path::Path::new(cwd).is_absolute())
            || fork.thread_source.as_ref().is_some_and(|source| !matches!(source, codex_app_server_protocol::ThreadSource::User))
        {
            return Err(invalid_request("portable import requires a fresh root fork, excludeTurns: true, matching source id and absolute destination cwd; paths, cutoffs, prediction, ephemeral and goal continuation are unavailable"));
        }
        let _import_permit = Arc::clone(&self.portable_history_exports.imports).try_acquire_owned()
            .map_err(|_| invalid_request("another portable history import is already being prepared"))?;
        let portable = codex_thread_store::portable_history::read_import_replay(
            params.path.into_path_buf(), params.byte_count, params.sha256.clone(), source_thread_id,
            self.config.codex_home.join("tmp").join("portable-history").to_path_buf(),
        ).await.map_err(|error| invalid_request(format!("invalid portable history: {error}")))?;
        // Everything above is read-only validation. Once fork startup is entered, an unclassified
        // failure/cancellation may already have persisted a new identity. Never relabel it retryable.
        Box::pin(self.thread_fork_inner(
            request_id, params.fork, app_server_client_name, app_server_client_version,
            client_mcp_extensions, Some(portable),
        )).await.map_err(|mut error| {
            error.data = Some(serde_json::json!({ "portableHistory": {
                "outcome": "unknown", "sourceThreadId": params.source_thread_id,
                "sha256": params.sha256,
            }, "underlying": error.data }));
            error
        })?;
        Ok(None)
    }
}
