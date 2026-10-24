use std::io::BufReader;
use std::io::Read;
use std::path::Path;

use codex_app_server_protocol::PORTABLE_HISTORY_MAX_LINE_BYTES;
use codex_app_server_protocol::PORTABLE_HISTORY_ROLLOUT_SCHEMA;
use codex_app_server_protocol::PORTABLE_HISTORY_VERSION;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;

use super::LocalThreadStore;
use super::paginated_fork;
use super::rollout_lineage::RolloutLineage;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::ReadThreadParams;
use crate::ThreadStoreResult;
use crate::portable_history::ArtifactWriter;
use crate::portable_history::FORMAT;
use crate::portable_history::PortableHistoryArtifact;
use crate::portable_history::PortableHistoryManifest;
use crate::portable_history::PortableHistorySegment;
use crate::portable_history::TerminalBoundary;
use crate::portable_history::cancelled;
use crate::portable_history::ensure_lossless;
use crate::portable_history::invalid;
use crate::portable_history::io_error;
use crate::portable_history::read_line;
use crate::portable_history::validate_manifest;

#[cfg(test)]
#[path = "portable_history_tests.rs"]
mod tests;

pub(super) async fn export(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<PortableHistoryArtifact> {
    // Graph export/import needs its own canonical identity/edge/history reconstruction. Do not
    // silently turn a multi-agent tree into one orphaned parent replay.
    let state_db = store.state_db.as_ref().ok_or_else(|| invalid("portable history needs local state"))?;
    if !state_db.list_thread_spawn_descendants(thread_id).await.map_err(io_error)?.is_empty() {
        return Err(crate::ThreadStoreError::Unsupported { operation: "portable_history_agent_graph" });
    }
    // Reuse the same source lifecycle reservation, persist barrier, projection and frozen physical
    // boundary as a normal paginated fork. The lease is held until the immutable export is durable.
    let prepared = paginated_fork::prepare(store, PrepareForkParams {
        thread_id, boundary: ForkBoundary::Latest,
    }).await?;
    let metadata = super::read_thread::read_thread(store, ReadThreadParams {
        thread_id, include_archived: true, include_history: false,
    }).await?;
    if metadata.history_mode != ThreadHistoryMode::Paginated
        || metadata.parent_thread_id.is_some()
        || metadata.source.is_non_root_agent()
    {
        return Err(invalid("portable history v1 exports only durable paginated root conversations"));
    }
    if !state_db.list_thread_spawn_descendants(thread_id).await.map_err(io_error)?.is_empty() {
        return Err(crate::ThreadStoreError::Unsupported { operation: "portable_history_agent_graph" });
    }
    let header = prepared.model_context.first().and_then(|item| match item {
        RolloutItem::SessionMeta(meta) => Some(meta.clone()),
        _ => None,
    }).ok_or_else(|| invalid("portable history source has no canonical metadata"))?;
    let lineage = store.resolve_rollout_lineage_for_reference(thread_id).await?;
    let lineage = match prepared.history_base {
        Some(boundary) => Some(lineage.truncate_at(boundary).await?),
        None => None,
    };
    let segments = lineage.as_ref().map(|lineage| {
        lineage.segments().iter().map(|segment| {
            let end = segment.end.ok_or_else(|| invalid("portable lineage is not frozen"))?;
            Ok(PortableHistorySegment {
                rollout_id: segment.rollout_id(), start_ordinal: segment.start_ordinal(),
                end_ordinal_exclusive: end.end_ordinal_exclusive,
                end_byte_offset: end.end_byte_offset,
            })
        }).collect::<ThreadStoreResult<Vec<_>>>()
    }).transpose()?.unwrap_or_default();
    let manifest = PortableHistoryManifest {
        format: FORMAT.to_string(), version: PORTABLE_HISTORY_VERSION,
        rollout_schema: PORTABLE_HISTORY_ROLLOUT_SCHEMA.to_string(), source_thread_id: thread_id,
        source_name: metadata.name, source_preview: metadata.preview, source_cwd: metadata.cwd,
        segments,
    };
    validate_manifest(&manifest, thread_id)?;
    let (stop, keep_running) = tokio::sync::oneshot::channel::<()>();
    let staging_directory = store.config.codex_home.join("tmp").join("portable-history");
    let result = tokio::task::spawn_blocking(move || {
        // Cancellation of the waiting RPC still leaves source ownership with this worker until
        // its bounded reader observes cancellation and deletes its own unfinished temporary file.
        let _source_reservation = prepared;
        write_export(manifest, header, lineage, &staging_directory, || stop.is_closed())
    }).await.map_err(io_error)?;
    drop(keep_running);
    result
}

fn write_export(
    manifest: PortableHistoryManifest,
    mut header: Box<codex_protocol::protocol::SessionMetaLine>,
    lineage: Option<RolloutLineage>,
    staging_directory: &Path,
    is_cancelled: impl Fn() -> bool,
) -> ThreadStoreResult<PortableHistoryArtifact> {
    cancelled(&is_cancelled)?;
    if header.meta.parent_thread_id.is_some() || header.meta.subagent_history_start_ordinal.is_some() {
        return Err(invalid("portable history cannot promote a subagent into a root conversation"));
    }
    // The artifact is a complete replay, so no foreign absolute path/history_base dependency
    // remains. Retain immediate-source runtime metadata/version rather than an ancestor's header.
    header.meta.history_base = None;
    chrono::DateTime::parse_from_rfc3339(&header.meta.timestamp).map_err(io_error)?;
    let mut writer = ArtifactWriter::new(staging_directory)?;
    writer.write_json(&manifest)?;
    writer.write_record(RolloutLine {
        timestamp: header.meta.timestamp.clone(), ordinal: Some(0),
        item: RolloutItem::SessionMeta(header),
    })?;
    let mut terminal = TerminalBoundary::default();
    if let Some(lineage) = lineage {
        for segment in lineage.segments() {
            cancelled(&is_cancelled)?;
            let end = segment.end.ok_or_else(|| invalid("portable lineage is not frozen"))?;
            let input = codex_rollout::open_rollout_stream(&segment.rollout_path).map_err(io_error)?;
            let mut reader = BufReader::new(input.take(end.end_byte_offset));
            let mut consumed = 0_u64;
            let mut next_ordinal = segment.start_ordinal().checked_sub(1)
                .ok_or_else(|| invalid("portable lineage has an invalid starting ordinal"))?;
            let mut first = true;
            loop {
                cancelled(&is_cancelled)?;
                let bytes = read_line(&mut reader, PORTABLE_HISTORY_MAX_LINE_BYTES)?;
                if bytes.is_empty() { break; }
                consumed = consumed.checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid("portable lineage byte offset overflow"))?;
                if bytes.iter().all(u8::is_ascii_whitespace) { continue; }
                let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(io_error)?;
                let line = codex_rollout::decode_rollout_line(value.clone()).map_err(io_error)?;
                ensure_lossless(&value, &serde_json::to_value(&line).map_err(io_error)?)?;
                chrono::DateTime::parse_from_rfc3339(&line.timestamp).map_err(io_error)?;
                let ordinal = line.ordinal.ok_or_else(|| invalid("portable lineage record has no ordinal"))?;
                // Gaps are legal in the existing projector, but regression/reuse and records past
                // the inherited prefix are not. Exact byte AND ordinal fences must agree.
                if ordinal < next_ordinal || ordinal >= end.end_ordinal_exclusive {
                    return Err(invalid("portable lineage ordinal/cutoff mismatch"));
                }
                if first {
                    if ordinal != next_ordinal || !matches!(&line.item, RolloutItem::SessionMeta(_)) {
                        return Err(invalid("portable lineage segment has no canonical header"));
                    }
                    first = false;
                }
                next_ordinal = ordinal.checked_add(1).ok_or_else(|| invalid("portable lineage ordinal overflow"))?;
                // Every segment contributes its local delta. Historical copied metadata does not
                // become a second canonical header in the flattened replay.
                if !matches!(&line.item, RolloutItem::SessionMeta(_)) {
                    terminal.observe(&line.item)?;
                    writer.write_record(line)?;
                }
            }
            if first || consumed != end.end_byte_offset || next_ordinal != end.end_ordinal_exclusive {
                return Err(invalid("portable lineage byte/ordinal fences do not identify a complete prefix"));
            }
        }
    }
    terminal.finish()?;
    writer.finish(manifest, terminal.last_terminal_turn_id)
}
