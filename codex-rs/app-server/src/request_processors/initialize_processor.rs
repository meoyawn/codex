use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use axum::http::HeaderValue;
use codex_analytics::AppServerRpcTransport;
use codex_login::default_client::SetOriginatorError;
use codex_login::default_client::USER_AGENT_SUFFIX;
use codex_login::default_client::get_codex_user_agent;
use codex_login::default_client::set_default_client_residency_requirement;
use codex_login::default_client::set_default_originator;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::mcp::OPENAI_ELICITATION_EXTENSION_ID;

use super::*;
use crate::message_processor::ConnectionSessionState;
use crate::message_processor::InitializedConnectionSessionState;
use crate::transport::ConnectionOrigin;

const NON_ORIGINATING_CLIENT_NAMES: &[&str] = &["codex_app_server_daemon", "codex-backend"];

#[derive(Clone)]
pub(crate) struct InitializeRequestProcessor {
    gateway_login_control: Arc<codex_login::GatewayLoginControl>,
    gateway_login_initialized: Arc<std::sync::OnceLock<()>>,
    outgoing: Arc<OutgoingMessageSender>,
    analytics_events_client: AnalyticsEventsClient,
    config: Arc<Config>,
    rpc_transport: AppServerRpcTransport,
    user_verification: Arc<crate::user_verification::Service>,
    portable_history_available: bool,
}

impl InitializeRequestProcessor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        gateway_login_control: Arc<codex_login::GatewayLoginControl>,
        outgoing: Arc<OutgoingMessageSender>,
        analytics_events_client: AnalyticsEventsClient,
        config: Arc<Config>,
        rpc_transport: AppServerRpcTransport,
        user_verification: Arc<crate::user_verification::Service>,
        portable_history_available: bool,
    ) -> Self {
        Self {
            gateway_login_control,
            gateway_login_initialized: Arc::new(std::sync::OnceLock::new()),
            outgoing,
            analytics_events_client,
            config,
            rpc_transport,
            user_verification,
            portable_history_available,
        }
    }

    pub(crate) async fn initialize(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: InitializeParams,
        session: &ConnectionSessionState,
        // `Some(...)` means the caller wants initialize to immediately mark the
        // connection outbound-ready. Websocket JSON-RPC calls pass `None` so
        // lib.rs can deliver connection-scoped initialize notifications first.
        outbound_initialized: Option<&AtomicBool>,
    ) -> Result<bool, JSONRPCErrorError> {
        let connection_request_id = ConnectionRequestId {
            connection_id,
            request_id,
        };
        if session.initialized() {
            return Err(invalid_request("Already initialized"));
        }

        // TODO(maxj): Revisit capability scoping for `experimental_api_enabled`.
        // Current behavior is per-connection. Reviewer feedback notes this can
        // create odd cross-client behavior (for example dynamic tool calls on a
        // shared thread when another connected client did not opt into
        // experimental API). Proposed direction is instance-global first-write-wins
        // with initialize-time mismatch rejection.
        let analytics_initialize_params = params.clone();
        let capabilities = params.capabilities.unwrap_or_default();
        let experimental_api_enabled = capabilities.experimental_api;
        let request_attestation = capabilities.request_attestation;
        let extensions = capabilities.extensions.as_ref();
        let client_tools = extensions.and_then(|extensions| extensions.get("dev.harness.clientTools"))
            .filter(|offer| experimental_api_enabled && offer.get("version").and_then(serde_json::Value::as_u64) == Some(1))
            .map(|_| codex_app_server_protocol::ClientToolsCapability { version: 1, dynamic_tools_on_resume: true, in_process_app_mcp: true });
        let harness_build = extensions.and_then(|extensions| extensions.get("dev.harness.buildIdentity"))
            .filter(|offer| offer.get("version").and_then(serde_json::Value::as_u64) == Some(1))
            .and_then(|_| Some(codex_app_server_protocol::HarnessBuildIdentity {
                version: 1,
                source_commit: option_env!("STABLE_GIT_COMMIT")?.to_string(),
                source_fingerprint_sha256: option_env!("HARNESS_SOURCE_FINGERPRINT_SHA256")?.to_string(),
                package_version: env!("CARGO_PKG_VERSION").to_string(),
                cargo_profile: option_env!("HARNESS_CARGO_PROFILE")?.to_string(),
            }));
        let portable_history_enabled = self.portable_history_available && experimental_api_enabled
            && extensions.and_then(|extensions| extensions.get(codex_app_server_protocol::PORTABLE_HISTORY_EXTENSION))
                .is_some_and(|offer| {
                    offer.get("version").and_then(serde_json::Value::as_u64) == Some(u64::from(codex_app_server_protocol::PORTABLE_HISTORY_VERSION))
                        && offer.get("rolloutSchema").and_then(serde_json::Value::as_str) == Some(codex_app_server_protocol::PORTABLE_HISTORY_ROLLOUT_SCHEMA)
                });
        let mut client_mcp_extensions = codex_mcp::client_mcp_extensions(
            extensions,
            capabilities.mcp_server_openai_form_elicitation,
        );
        let opt_out_notification_methods = capabilities
            .opt_out_notification_methods
            .unwrap_or_default();
        let ClientInfo {
            name,
            title: _title,
            version,
        } = params.client_info;
        // Validate before committing; set_default_originator validates while
        // mutating process-global metadata.
        if HeaderValue::from_str(&name).is_err() {
            return Err(invalid_request(format!(
                "Invalid clientInfo.name: '{name}'. Must be a valid HTTP header value."
            )));
        }
        // Activate only the embedded TUI and local desktop host. Client-supplied
        // extensions cannot opt other hosts into verification.
        let user_verification_enabled = experimental_api_enabled
            && matches!(
                (session.origin, name.as_str()),
                (ConnectionOrigin::InProcess, "codex-tui")
                    | (ConnectionOrigin::Stdio, "Codex Desktop")
            )
            && tokio::task::spawn_blocking(self.user_verification.device_supported)
                .await
                .unwrap_or(false);
        if user_verification_enabled {
            let mut extensions = client_mcp_extensions
                .iter()
                .map(|(id, value)| (id.to_string(), value.clone()))
                .collect::<std::collections::HashMap<_, _>>();
            let settings = extensions
                .entry(OPENAI_ELICITATION_EXTENSION_ID.to_string())
                .or_insert_with(|| serde_json::json!({}));
            if !settings.is_object() {
                *settings = serde_json::json!({});
            }
            settings["userVerification"] = serde_json::json!({});
            client_mcp_extensions = ClientMcpExtensions::new(extensions);
        }
        let originator = name.clone();
        let user_agent_suffix = format!("{name}; {version}");
        let mutates_global_identity = !NON_ORIGINATING_CLIENT_NAMES.contains(&name.as_str());
        let codex_home = self.config.codex_home.clone();
        if session
            .initialize(InitializedConnectionSessionState {
                experimental_api_enabled,
                portable_history_enabled,
                opted_out_notification_methods: opt_out_notification_methods.into_iter().collect(),
                app_server_client_name: name.clone(),
                client_version: version,
                request_attestation,
                client_mcp_extensions,
            })
            .is_err()
        {
            return Err(invalid_request("Already initialized"));
        }
        if user_verification_enabled {
            self.outgoing
                .enable_user_verification_connection(connection_id)
                .await;
        }

        if capabilities.explicit_gateway_oauth || mutates_global_identity {
            // Only the first originating client may restore legacy automatic login.
            // Any explicit opt-in is sticky across subsequent connections.
            self.gateway_login_initialized.get_or_init(|| {
                if !capabilities.explicit_gateway_oauth {
                    self.gateway_login_control.allow_automatic_login();
                }
            });
            if capabilities.explicit_gateway_oauth {
                self.gateway_login_control.require_explicit_login();
            }
        }

        if mutates_global_identity {
            // Only real client initialization may mutate process-global client metadata.
            if let Err(error) = set_default_originator(originator.clone()) {
                match error {
                    SetOriginatorError::InvalidHeaderValue => {
                        tracing::warn!(
                            client_info_name = %name,
                            "validated clientInfo.name was rejected while setting originator"
                        );
                    }
                    SetOriginatorError::AlreadyInitialized => {
                        // No-op. This is expected to happen if the originator is already set via env var.
                        // TODO(owen): Once we remove support for CODEX_INTERNAL_ORIGINATOR_OVERRIDE,
                        // this will be an unexpected state and we can return a JSON-RPC error indicating
                        // internal server error.
                    }
                }
            }
        }
        self.analytics_events_client.track_initialize(
            connection_id.0,
            analytics_initialize_params,
            originator,
            self.rpc_transport,
        );
        set_default_client_residency_requirement(self.config.enforce_residency.value());
        if mutates_global_identity && let Ok(mut suffix) = USER_AGENT_SUFFIX.lock() {
            *suffix = Some(user_agent_suffix);
        }

        #[cfg(windows)]
        if matches!(session.origin, ConnectionOrigin::Stdio) && name == "Codex Desktop" {
            // Uninstall ownership must not depend on account sign-in or sandbox setup.
            // Keep this bounded attempt ahead of the response; background registration can race uninstall.
            let home = codex_home.clone();
            if !matches!(
                tokio::task::spawn_blocking(move || {
                    codex_windows_sandbox::register_desktop_installation(&home)
                })
                .await,
                Ok(Ok(()))
            ) {
                tracing::warn!("could not register desktop uninstall ownership");
            }
        }

        let user_agent = get_codex_user_agent();
        let response = InitializeResponse {
            user_agent,
            codex_home,
            platform_family: std::env::consts::FAMILY.to_string(),
            platform_os: std::env::consts::OS.to_string(),
            portable_history: portable_history_enabled.then(codex_app_server_protocol::PortableHistoryCapability::v1),
            harness_build,
            client_tools,
        };

        self.outgoing
            .send_response(connection_request_id, response)
            .await;

        if let Some(outbound_initialized) = outbound_initialized {
            outbound_initialized.store(true, Ordering::Release);
            return Ok(true);
        }

        Ok(false)
    }

    pub(crate) fn track_initialized_request(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        request: &ClientRequest,
    ) {
        self.analytics_events_client
            .track_request(connection_id.0, request_id, request);
    }
}
