//! Local Harness extension. This is not an upstream/private OpenAI HTTP protocol.

use super::ThreadForkParams;
use crate::JsonSchema;
use crate::TS;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde::Serialize;

pub const PORTABLE_HISTORY_EXTENSION: &str = "dev.harness.portableHistory";
pub const PORTABLE_HISTORY_VERSION: u32 = 1;
// Both peers must implement this exact raw-rollout interpretation, not merely the same CLI version.
pub const PORTABLE_HISTORY_ROLLOUT_SCHEMA: &str = "codex-83086188ae0614bd882818ff1aa23f3194962012-harness-portable-v1";
pub const PORTABLE_HISTORY_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub const PORTABLE_HISTORY_MAX_LINE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct PortableHistoryCapability {
    pub version: u32,
    pub rollout_schema: String,
    pub max_bytes: u64,
    pub max_line_bytes: u64,
    /// V1 fails before exporting a thread with stored spawn descendants; it never drops them.
    pub supports_agent_graph: bool,
}

impl PortableHistoryCapability {
    pub fn v1() -> Self {
        Self {
            version: PORTABLE_HISTORY_VERSION,
            rollout_schema: PORTABLE_HISTORY_ROLLOUT_SCHEMA.to_string(),
            max_bytes: PORTABLE_HISTORY_MAX_BYTES,
            max_line_bytes: PORTABLE_HISTORY_MAX_LINE_BYTES,
            supports_agent_graph: false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadPortableHistoryExportParams {
    pub version: u32,
    pub rollout_schema: String,
    pub thread_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadPortableHistoryExportResponse {
    pub version: u32,
    pub rollout_schema: String,
    pub export_id: String,
    pub source_thread_id: String,
    /// Server-owned immutable file. Transport its exact bytes; do not edit or execute it.
    pub path: AbsolutePathBuf,
    pub byte_count: u64,
    pub sha256: String,
    pub record_count: u64,
    /// Last persisted terminal turn in the frozen complete replay; null for a turnless thread.
    pub terminal_turn_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadPortableHistoryReleaseParams {
    pub export_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[ts(export_to = "v2/")]
pub struct ThreadPortableHistoryReleaseResponse {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadPortableHistoryImportParams {
    pub version: u32,
    pub rollout_schema: String,
    /// Destination-local copy owned by the handoff operation, not a rollout/state DB path.
    pub path: AbsolutePathBuf,
    pub byte_count: u64,
    pub sha256: String,
    pub source_thread_id: String,
    /// Normal destination fork overrides/requirements. A nonempty absolute destination cwd is required.
    /// path, cutoffs, prediction mode, ephemeral and deferGoalContinuation must be absent/false.
    /// The imported snapshot is already complete and terminal. No goal or turn is started.
    pub fork: ThreadForkParams,
}
