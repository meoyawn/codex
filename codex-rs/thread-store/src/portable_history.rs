//! Self-contained local portable-history artifact. No state DB, credentials or UI snapshot is copied.

use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_app_server_protocol::PORTABLE_HISTORY_MAX_BYTES;
use codex_app_server_protocol::PORTABLE_HISTORY_MAX_LINE_BYTES;
use codex_app_server_protocol::PORTABLE_HISTORY_ROLLOUT_SCHEMA;
use codex_app_server_protocol::PORTABLE_HISTORY_VERSION;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub const FORMAT: &str = "dev.harness.portableHistory";
const MAX_HEADER_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PortableHistorySegment {
    pub rollout_id: ThreadId,
    pub start_ordinal: u64,
    pub end_ordinal_exclusive: u64,
    pub end_byte_offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PortableHistoryManifest {
    pub format: String,
    pub version: u32,
    pub rollout_schema: String,
    pub source_thread_id: ThreadId,
    pub source_name: Option<String>,
    pub source_preview: String,
    pub source_cwd: PathBuf,
    pub segments: Vec<PortableHistorySegment>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Footer {
    portable_history_record_count: u64,
    terminal_turn_id: Option<String>,
}

/// The temporary file is private, immutable after construction and deleted when this lease drops.
/// The processor retains the lease until explicit release, bounded expiry or shutdown.
pub struct PortableHistoryArtifact {
    pub manifest: PortableHistoryManifest,
    pub byte_count: u64,
    pub sha256: String,
    pub record_count: u64,
    pub terminal_turn_id: Option<String>,
    pub(crate) file: tempfile::NamedTempFile,
}

impl PortableHistoryArtifact {
    pub fn path(&self) -> &Path {
        self.file.path()
    }
}

pub struct ImportedPortableHistory {
    pub manifest: PortableHistoryManifest,
    pub lines: Vec<RolloutLine>,
}

pub struct ImportedPortableReplay {
    pub manifest: PortableHistoryManifest,
    pub model_context: Arc<Vec<RolloutItem>>,
    pub replay: PortableHistoryReplay,
}

/// Validated anonymous snapshot, read once in bounded batches during startup.
pub struct PortableHistoryReplay {
    reader: BufReader<File>,
    remaining: u64,
    pub record_count: u64,
}

impl PortableHistoryReplay {
    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    pub async fn next_batch(mut self) -> ThreadStoreResult<(Self, Vec<RolloutLine>)> {
        tokio::task::spawn_blocking(move || {
            let mut lines = Vec::new();
            let mut byte_count = 0;
            while self.remaining > 0 && lines.len() < 1024 && byte_count < 2 * 1024 * 1024 {
                let bytes = read_line(&mut self.reader, PORTABLE_HISTORY_MAX_LINE_BYTES)?;
                if bytes.is_empty() { return Err(invalid("validated portable snapshot was truncated")); }
                byte_count += bytes.len();
                lines.push(codex_rollout::decode_rollout_line(serde_json::from_slice(&bytes).map_err(io_error)?).map_err(io_error)?);
                self.remaining -= 1;
            }
            Ok((self, lines))
        }).await.map_err(io_error)?
    }
}

/// Read and hash one opened file into an anonymous snapshot before parsing it. Concurrent changes
/// to the caller's pathname cannot switch the validated input underneath fork startup.
pub async fn read_import(
    path: PathBuf,
    byte_count: u64,
    sha256: String,
    source_thread_id: ThreadId,
) -> ThreadStoreResult<ImportedPortableHistory> {
    let imported = read_import_replay(path, byte_count, sha256, source_thread_id, std::env::temp_dir()).await?;
    let mut replay = imported.replay;
    let mut lines = Vec::new();
    while !replay.is_empty() {
        let (next, batch) = replay.next_batch().await?;
        replay = next;
        lines.extend(batch);
    }
    Ok(ImportedPortableHistory { manifest: imported.manifest, lines })
}

pub async fn read_import_replay(
    path: PathBuf,
    byte_count: u64,
    sha256: String,
    source_thread_id: ThreadId,
    staging_directory: PathBuf,
) -> ThreadStoreResult<ImportedPortableReplay> {
    let (stop, keep_running) = tokio::sync::oneshot::channel::<()>();
    let result = tokio::task::spawn_blocking(move || {
        read_import_blocking(&path, byte_count, &sha256, source_thread_id, &staging_directory, || stop.is_closed())
    })
    .await
    .map_err(|error| invalid(format!("portable history reader failed: {error}")))?;
    drop(keep_running);
    result
}

fn read_import_blocking(
    path: &Path,
    byte_count: u64,
    sha256: &str,
    source_thread_id: ThreadId,
    staging_directory: &Path,
    is_cancelled: impl Fn() -> bool,
) -> ThreadStoreResult<ImportedPortableReplay> {
    if !path.is_absolute() || byte_count == 0 || byte_count > PORTABLE_HISTORY_MAX_BYTES {
        return Err(invalid("invalid portable history path/byte count"));
    }
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("invalid portable history digest"));
    }
    let mut input = File::open(path).map_err(io_error)?;
    let metadata = input.metadata().map_err(io_error)?;
    if !metadata.is_file() || metadata.len() != byte_count {
        return Err(invalid("portable history size changed or input is not a regular file"));
    }
    prepare_staging_directory(staging_directory)?;
    let mut snapshot = tempfile::tempfile_in(staging_directory).map_err(io_error)?;
    let mut digest = Sha256::new();
    let mut remaining = byte_count;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining != 0 {
        cancelled(&is_cancelled)?;
        let limit = usize::try_from(remaining.min(buffer.len() as u64)).map_err(io_error)?;
        let read = input.read(&mut buffer[..limit]).map_err(io_error)?;
        if read == 0 {
            return Err(invalid("portable history was truncated during validation"));
        }
        snapshot.write_all(&buffer[..read]).map_err(io_error)?;
        digest.update(&buffer[..read]);
        remaining -= read as u64;
    }
    if input.read(&mut buffer[..1]).map_err(io_error)? != 0
        || !format!("{:x}", digest.finalize()).eq_ignore_ascii_case(sha256)
    {
        return Err(invalid("portable history digest/size mismatch"));
    }
    snapshot.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let mut reader = BufReader::new(snapshot);
    let header = read_line(&mut reader, MAX_HEADER_BYTES)?;
    let manifest: PortableHistoryManifest = serde_json::from_slice(&header).map_err(io_error)?;
    validate_manifest(&manifest, source_thread_id)?;
    let records_start = reader.stream_position().map_err(io_error)?;
    let mut record_count = 0_u64;
    let mut session_meta = None;
    let mut terminal = TerminalBoundary::default();
    loop {
        cancelled(&is_cancelled)?;
        let record_start = reader.stream_position().map_err(io_error)?;
        let bytes = read_line(&mut reader, PORTABLE_HISTORY_MAX_LINE_BYTES)?;
        if bytes.is_empty() {
            return Err(invalid("portable history footer is missing"));
        }
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(io_error)?;
        if value.get("portableHistoryRecordCount").is_some() {
            let footer: Footer = serde_json::from_value(value).map_err(io_error)?;
            if footer.portable_history_record_count != record_count || record_count == 0 {
                return Err(invalid("portable history record count mismatch"));
            }
            if !read_line(&mut reader, 1)?.is_empty() {
                return Err(invalid("unexpected bytes after portable history footer"));
            }
            terminal.finish()?;
            if footer.terminal_turn_id != terminal.last_terminal_turn_id {
                return Err(invalid("portable history terminal boundary mismatch"));
            }
            let mut snapshot = reader.into_inner();
            // Use precisely the same compaction-aware startup scan as native
            // paginated resumes. The complete older transcript stays on disk.
            let mut scan = codex_rollout::ModelContextScan::default();
            {
                let mut scanner = codex_rollout::ReverseJsonlScanner::new_at(&mut snapshot, record_start).map_err(io_error)?;
                while let Some(outcome) = scanner.scan_next_rollout_line().map_err(io_error)? {
                    cancelled(&is_cancelled)?;
                    let codex_rollout::ScanOutcome::Parsed(line) = outcome else {
                        return Err(invalid("validated portable snapshot contains an unreadable record"));
                    };
                    if matches!(&line.item, RolloutItem::SessionMeta(_)) { break; }
                    if scan.push(line.item) == codex_rollout::ModelContextScanProgress::Complete { break; }
                }
            }
            let mut model_context = scan.finish();
            model_context.insert(0, RolloutItem::SessionMeta(session_meta.ok_or_else(|| invalid("portable history has no canonical header"))?));
            snapshot.seek(SeekFrom::Start(records_start)).map_err(io_error)?;
            return Ok(ImportedPortableReplay { manifest, model_context: Arc::new(model_context),
                replay: PortableHistoryReplay { reader: BufReader::new(snapshot), remaining: record_count, record_count } });
        }
        let line = codex_rollout::decode_rollout_line(value.clone()).map_err(io_error)?;
        ensure_lossless(&value, &serde_json::to_value(&line).map_err(io_error)?)?;
        if line.ordinal != Some(record_count) {
            return Err(invalid("portable history ordinals are not contiguous"));
        }
        chrono::DateTime::parse_from_rfc3339(&line.timestamp).map_err(io_error)?;
        match &line.item {
            RolloutItem::SessionMeta(meta) if record_count == 0 => {
                if meta.meta.id != source_thread_id
                    || meta.meta.history_mode != ThreadHistoryMode::Paginated
                    || meta.meta.history_base.is_some()
                    || meta.meta.parent_thread_id.is_some()
                    || meta.meta.subagent_history_start_ordinal.is_some()
                    || meta.meta.source.is_non_root_agent()
                {
                    return Err(invalid("portable history has an invalid canonical root header"));
                }
                session_meta = Some(meta.clone());
            }
            RolloutItem::SessionMeta(_) => {
                return Err(invalid("portable history contains an unflattened session header"));
            }
            _ if record_count == 0 => return Err(invalid("portable history has no canonical header")),
            _ => terminal.observe(&line.item)?,
        }
        record_count = record_count.checked_add(1).ok_or_else(|| invalid("portable history record count overflow"))?;
    }
}

pub(crate) fn validate_manifest(
    manifest: &PortableHistoryManifest,
    source_thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    if manifest.format != FORMAT
        || manifest.version != PORTABLE_HISTORY_VERSION
        || manifest.rollout_schema != PORTABLE_HISTORY_ROLLOUT_SCHEMA
        || manifest.source_thread_id != source_thread_id
    {
        return Err(invalid("unsupported portable history schema or source identity"));
    }
    if serde_json::to_vec(manifest).map_err(io_error)?.len() as u64 + 1 > MAX_HEADER_BYTES {
        return Err(invalid("portable history metadata exceeds the header byte limit"));
    }
    let mut seen = HashSet::new();
    let mut previous_end = None;
    for segment in &manifest.segments {
        if !seen.insert(segment.rollout_id)
            || segment.start_ordinal == 0
            || segment.end_ordinal_exclusive < segment.start_ordinal
            || segment.end_byte_offset == 0
            || previous_end.is_some_and(|end: u64| end.checked_add(1) != Some(segment.start_ordinal))
        {
            return Err(invalid("invalid portable history lineage boundaries"));
        }
        if previous_end.is_none() && segment.start_ordinal != 1 {
            return Err(invalid("portable history omits its oldest lineage prefix"));
        }
        previous_end = Some(segment.end_ordinal_exclusive);
    }
    Ok(())
}

/// Fail instead of silently dropping fields/variants that the destination's typed replay cannot
/// represent. Missing historical optional/default fields are accepted; changed values are not.
pub(crate) fn ensure_lossless(
    source: &serde_json::Value,
    decoded: &serde_json::Value,
) -> ThreadStoreResult<()> {
    match (source, decoded) {
        (serde_json::Value::Object(source), serde_json::Value::Object(decoded)) => {
            for (key, value) in source {
                match decoded.get(key) {
                    Some(decoded) => ensure_lossless(value, decoded)?,
                    None if value.is_null() => {}
                    None => return Err(invalid("portable history contains an unsupported record field")),
                }
            }
        }
        (serde_json::Value::Array(source), serde_json::Value::Array(decoded)) => {
            if source.len() != decoded.len() {
                return Err(invalid("portable history record cannot be replayed losslessly"));
            }
            for (source, decoded) in source.iter().zip(decoded) {
                ensure_lossless(source, decoded)?;
            }
        }
        _ if source == decoded => {}
        _ => return Err(invalid("portable history record cannot be replayed losslessly")),
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct TerminalBoundary {
    open_turns: HashSet<String>,
    pub(crate) last_terminal_turn_id: Option<String>,
    user_input_after_boundary: bool,
}

impl TerminalBoundary {
    pub(crate) fn observe(&mut self, item: &RolloutItem) -> ThreadStoreResult<()> {
        match item {
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                if !self.open_turns.insert(event.turn_id.clone()) {
                    return Err(invalid("portable history contains an overlapping turn start"));
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                self.open_turns.remove(&event.turn_id);
                self.last_terminal_turn_id = Some(event.turn_id.clone());
                self.user_input_after_boundary = false;
            }
            RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => match event.turn_id.as_ref() {
                Some(id) => { self.open_turns.remove(id); self.last_terminal_turn_id = Some(id.clone()); self.user_input_after_boundary = false; }
                None if self.open_turns.is_empty() && !self.user_input_after_boundary => {}
                None => return Err(invalid("portable paginated history has an aborted turn without a canonical turn id")),
            },
            RolloutItem::ResponseItem(response)
                if matches!(&response.item, codex_protocol::models::ResponseItem::Message { role, .. } if role == "user") => {
                self.user_input_after_boundary = true;
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => self.user_input_after_boundary = true,
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                if matches!(&event.item, codex_protocol::items::TurnItem::UserMessage(_)) => {
                self.user_input_after_boundary = true;
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn finish(&self) -> ThreadStoreResult<()> {
        if self.open_turns.is_empty() && !self.user_input_after_boundary {
            Ok(())
        } else {
            Err(ThreadStoreError::Conflict {
                message: "portable history requires a persisted terminal turn boundary".to_string(),
            })
        }
    }
}

pub(crate) fn read_line(reader: &mut impl BufRead, limit: u64) -> ThreadStoreResult<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit + 1).read_until(b'\n', &mut bytes).map_err(io_error)?;
    if bytes.len() as u64 > limit || (!bytes.is_empty() && !bytes.ends_with(b"\n")) {
        return Err(invalid("portable history has an oversized or incomplete record"));
    }
    Ok(bytes)
}

fn prepare_staging_directory(path: &Path) -> ThreadStoreResult<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path).map_err(io_error)
}

pub(crate) struct ArtifactWriter {
    pub(crate) file: tempfile::NamedTempFile,
    digest: Sha256,
    byte_count: u64,
    record_count: u64,
}

impl ArtifactWriter {
    pub(crate) fn new(staging_directory: &Path) -> ThreadStoreResult<Self> {
        prepare_staging_directory(staging_directory)?;
        Ok(Self {
            file: tempfile::Builder::new().prefix("harness-portable-history-").suffix(".jsonl")
                .tempfile_in(staging_directory).map_err(io_error)?,
            digest: Sha256::new(), byte_count: 0, record_count: 0,
        })
    }

    pub(crate) fn write_json(&mut self, value: &impl Serialize) -> ThreadStoreResult<()> {
        let mut bytes = serde_json::to_vec(value).map_err(io_error)?;
        bytes.push(b'\n');
        self.byte_count = self.byte_count.checked_add(bytes.len() as u64)
            .filter(|count| *count <= PORTABLE_HISTORY_MAX_BYTES)
            .ok_or_else(|| invalid("portable history exceeds the byte limit"))?;
        if bytes.len() as u64 > PORTABLE_HISTORY_MAX_LINE_BYTES {
            return Err(invalid("portable history exceeds the record byte limit"));
        }
        self.file.write_all(&bytes).map_err(io_error)?;
        self.digest.update(&bytes);
        Ok(())
    }

    pub(crate) fn write_record(&mut self, mut line: RolloutLine) -> ThreadStoreResult<()> {
        line.ordinal = Some(self.record_count);
        self.write_json(&line)?;
        self.record_count = self.record_count.checked_add(1).ok_or_else(|| invalid("portable history record count overflow"))?;
        Ok(())
    }

    pub(crate) fn finish(mut self, manifest: PortableHistoryManifest, terminal_turn_id: Option<String>) -> ThreadStoreResult<PortableHistoryArtifact> {
        self.write_json(&Footer { portable_history_record_count: self.record_count, terminal_turn_id: terminal_turn_id.clone() })?;
        self.file.as_file().sync_all().map_err(io_error)?;
        Ok(PortableHistoryArtifact {
            manifest, byte_count: self.byte_count, sha256: format!("{:x}", self.digest.finalize()),
            record_count: self.record_count, terminal_turn_id, file: self.file,
        })
    }
}

pub(crate) fn cancelled(is_cancelled: &impl Fn() -> bool) -> ThreadStoreResult<()> {
    if is_cancelled() {
        return Err(invalid("portable history operation was cancelled"));
    }
    Ok(())
}

pub(crate) fn invalid(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest { message: message.into() }
}

pub(crate) fn io_error(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal { message: format!("portable history: {error}") }
}
