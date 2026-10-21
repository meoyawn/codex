use std::fs;
use std::path::Path;
use std::sync::Arc;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use sha2::Digest;
use tempfile::TempDir;

use super::*;
use crate::ThreadStore;
use crate::CreateThreadParams;
use crate::LiveThread;
use crate::LiveThreadInitGuard;
use crate::ThreadPersistenceMetadata;
use crate::local::test_support::test_config;
use crate::portable_history::read_import;

const TIMESTAMP: &str = "2026-07-16T00:00:00.000Z";

async fn store(home: &Path) -> LocalThreadStore {
    let config = test_config(home);
    let state = codex_state::StateRuntime::init(config.sqlite.clone(), "test-provider".into())
        .await.expect("state database");
    LocalThreadStore::new(config, Some(state))
}

async fn seed(
    store: &LocalThreadStore,
    id: ThreadId,
    base: Option<HistoryPosition>,
    items: Vec<RolloutItem>,
) -> (std::path::PathBuf, Vec<RolloutLine>) {
    let initial = base.map_or(0, |base| base.end_ordinal_exclusive);
    let header = RolloutItem::SessionMeta(SessionMetaLine {
        meta: SessionMeta {
            id, session_id: id.into(), timestamp: TIMESTAMP.into(),
            cwd: store.config.codex_home.clone(), source: SessionSource::Cli,
            history_mode: ThreadHistoryMode::Paginated, history_base: base,
            ..Default::default()
        },
        git: None,
    });
    let lines: Vec<_> = std::iter::once(header).chain(items).enumerate().map(|(index, item)| RolloutLine {
        timestamp: TIMESTAMP.into(), ordinal: Some(initial + index as u64), item,
    }).collect();
    let directory = store.config.codex_home.join("sessions/2026/07/16");
    fs::create_dir_all(&directory).expect("sessions directory");
    let path = directory.join(format!("rollout-2026-07-16T00-00-00-{id}.jsonl"));
    let bytes = lines.iter().map(|line| serde_json::to_string(line).unwrap() + "\n").collect::<String>();
    fs::write(&path, bytes).expect("write source rollout");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(id, path.clone(), chrono::Utc::now(), SessionSource::Cli);
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = store.config.codex_home.clone();
    store.state_db.as_ref().unwrap().upsert_thread(&metadata.build("test-provider"))
        .await.expect("register source");
    (path, lines)
}

fn event(value: serde_json::Value) -> RolloutItem {
    RolloutItem::EventMsg(serde_json::from_value(value).expect("event fixture"))
}

fn completed_turn(id: &str) -> Vec<RolloutItem> {
    vec![
        event(serde_json::json!({"type":"task_started", "turn_id":id, "model_context_window":null})),
        event(serde_json::json!({"type":"task_complete", "turn_id":id, "last_agent_message":null})),
    ]
}

#[tokio::test]
async fn portable_history_flattens_frozen_ancestors_and_survives_return_trip() {
    let source_home = TempDir::new().unwrap();
    let destination_home = TempDir::new().unwrap();
    let source = store(source_home.path()).await;
    let destination = store(destination_home.path()).await;
    let root = ThreadId::default();
    let child = ThreadId::default();
    let (root_path, root_lines) = seed(&source, root, None, completed_turn("inherited")).await;
    let frozen = HistoryPosition {
        thread_id: root, end_ordinal_exclusive: root_lines.len() as u64,
        end_byte_offset: fs::metadata(&root_path).unwrap().len(),
    };
    // A later ancestor turn must never leak into its child's frozen history.
    let mut extended = root_lines;
    for item in completed_turn("after-fork") {
        extended.push(RolloutLine { timestamp: TIMESTAMP.into(), ordinal: Some(extended.len() as u64), item });
    }
    fs::write(&root_path, extended.iter().map(|line| serde_json::to_string(line).unwrap() + "\n").collect::<String>()).unwrap();
    let compaction = RolloutItem::Compacted(codex_rollout::CompactedItem {
        message: "compacted inherited context".into(), replacement_history: Some(Vec::new()),
        retained_context: None, guardian_history: None, mcp_resource_origins: None,
        window_number: Some(1), first_window_id: None, previous_window_id: None, window_id: None,
        compaction_response_id: None, latest_token_usage_record: None, resume_metadata: None,
    });
    let mut items = vec![compaction];
    items.extend(completed_turn("local"));
    let (_, suffix) = seed(&source, child, Some(frozen), items).await;
    let exported = source.export_portable_history(child).await.expect("export complete lineage");
    let imported = read_import(exported.path().into(), exported.byte_count, exported.sha256.clone(), child)
        .await.expect("validate portable replay");
    assert_eq!(imported.lines.len(), 6);
    assert_eq!(imported.manifest.source_thread_id, child);
    assert_eq!(exported.terminal_turn_id.as_deref(), Some("local"));
    assert!(imported.lines.iter().all(|line| line.timestamp == TIMESTAMP));
    assert!(!serde_json::to_string(&imported.lines).unwrap().contains("after-fork"));
    assert_eq!(serde_json::to_value(&imported.lines[3].item).unwrap(), serde_json::to_value(&suffix[1].item).unwrap());
    let RolloutItem::SessionMeta(meta) = &imported.lines[0].item else { panic!("canonical header") };
    assert_eq!(meta.meta.id, child);
    assert_eq!(meta.meta.history_base, None);

    // Persist the opaque replay on another host, then export it back. No source
    // rollout files or foreign history_base pointers exist at the destination.
    let received_id = ThreadId::default();
    let mut guard = LiveThreadInitGuard::default();
    let live = LiveThread::create_with_portable_history(Arc::new(destination.clone()), CreateThreadParams {
        creator_user_id: None, creator_account_id: None, session_id: received_id.into(), thread_id: received_id,
        extra_config: None, forked_from_id: Some(child), parent_thread_id: None, source: SessionSource::Cli,
        thread_source: None, originator: "harness-test".into(), base_instructions: Default::default(),
        dynamic_tools: Vec::new(), selected_capability_roots: Vec::new(), multi_agent_version: None,
        history_mode: ThreadHistoryMode::Paginated, history_base: None, subagent_history_start_ordinal: None,
        initial_window_id: "window-1".into(), runtime_workspace_roots: None,
        metadata: ThreadPersistenceMetadata {
            cwd: Some(destination_home.path().into()), model_provider: "test-provider".into(),
            memory_mode: codex_protocol::protocol::ThreadMemoryMode::Enabled,
        },
    }, imported.lines, &mut guard).await.expect("persist imported replay");
    guard.commit();
    live.shutdown().await.expect("close imported writer");
    let durable = live.read_thread(true, false).await.expect("cold read imported replay");
    assert_eq!(durable.thread_id, received_id);
    assert_eq!(durable.history_mode, ThreadHistoryMode::Paginated);
    let path = durable.rollout_path.expect("destination rollout");
    let persisted: Vec<RolloutLine> = fs::read_to_string(path).unwrap().lines()
        .map(|line| codex_rollout::decode_rollout_line(serde_json::from_str(line).unwrap()).unwrap()).collect();
    assert!(persisted.iter().filter(|line| !matches!(line.item, RolloutItem::SessionMeta(_))).all(|line| line.timestamp == TIMESTAMP));
    let returned = destination.export_portable_history(received_id).await.expect("reverse export");
    let replay = read_import(returned.path().into(), returned.byte_count, returned.sha256.clone(), received_id)
        .await.expect("reverse import");
    assert_eq!(replay.lines.len(), 6);
    assert_eq!(returned.terminal_turn_id.as_deref(), Some("local"));
    assert_ne!(replay.manifest.source_thread_id, child);
}

#[tokio::test]
async fn portable_history_rejects_tampering_and_unrepresentable_records() {
    let home = TempDir::new().unwrap();
    let source = store(home.path()).await;
    let id = ThreadId::default();
    seed(&source, id, None, completed_turn("finished")).await;
    let artifact = source.export_portable_history(id).await.unwrap();
    let original = fs::read(artifact.path()).unwrap();
    let path = home.path().join("received.jsonl");
    let mut rows: Vec<serde_json::Value> = original.split(|byte| *byte == b'\n').filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap()).collect();
    rows[2]["payload"]["unknown_replay_field"] = serde_json::json!(true);
    let changed = rows.iter().map(|row| row.to_string() + "\n").collect::<String>();
    fs::write(&path, changed.as_bytes()).unwrap();
    let error = read_import(path.clone(), changed.len() as u64, artifact.sha256.clone(), id)
        .await.err().expect("digest must be checked");
    assert!(error.to_string().contains("digest/size mismatch"));
    let digest = format!("{:x}", sha2::Sha256::digest(changed.as_bytes()));
    let error = read_import(path, changed.len() as u64, digest, id)
        .await.err().expect("unknown typed fields must not be dropped");
    assert!(error.to_string().contains("unsupported record field"));
}

#[tokio::test]
async fn portable_history_refuses_a_nonterminal_source() {
    let home = TempDir::new().unwrap();
    let source = store(home.path()).await;
    let id = ThreadId::default();
    let mut items = completed_turn("finished");
    items.push(completed_turn("still-running").remove(0));
    seed(&source, id, None, items).await;
    let error = source.export_portable_history(id).await.err().expect("active prefix must be refused");
    assert!(error.to_string().contains("persisted terminal turn boundary"));
}
