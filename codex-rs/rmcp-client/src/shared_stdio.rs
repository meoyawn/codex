//! Process-owned MCP subprocesses shared by configuration declaration.
//!
//! Threads keep independent RMCP clients, approvals and cancellation. The owner
//! holds the subprocess and routes complete RPC exchanges over in-memory pipes.
//! Legacy exchanges are serialized because their callbacks need not carry a
//! parent request ID; no callback can be attributed to another conversation.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rmcp::transport::Transport;
use futures::FutureExt;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::Instant;

use crate::bounded_stdio_transport::stdio_line_limit;
use crate::protocol_mode::McpProtocolMode;
use crate::stdio_server_launcher::{LocalStdioServerLauncher, StdioServerCommand, StdioServerTransport};

const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const START_TIMEOUT: Duration = Duration::from_secs(15);

/// A global declaration has the same identity in every project. A project
/// declaration uses its own config file and project root, even for equal commands.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct McpStdioScope {
    file: PathBuf,
    name: String,
    profile: Option<String>,
    pub cwd: PathBuf,
}

impl McpStdioScope {
    pub fn new(file: PathBuf, name: String, profile: Option<String>, cwd: PathBuf) -> Self {
        Self { file: file.canonicalize().unwrap_or(file), name, profile, cwd: cwd.canonicalize().unwrap_or(cwd) }
    }
}

#[derive(Clone, PartialEq)]
struct ServerSpec {
    scope: McpStdioScope,
    command: StdioServerCommand,
    request_timeout: Duration,
}

struct Owner {
    command: StdioServerCommand,
    events: mpsc::Sender<Event>,
}

static OWNERS: OnceLock<Mutex<HashMap<McpStdioScope, Owner>>> = OnceLock::new();
static CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub(super) async fn connect(scope: McpStdioScope, command: StdioServerCommand, request_timeout: Duration, reconnect: bool) -> io::Result<StdioServerTransport> {
    let spec = ServerSpec { scope, command, request_timeout };
    let events = {
        let mut owners = OWNERS.get_or_init(Default::default).lock().await;
        if reconnect && owners.get(&spec.scope).is_some_and(|owner| owner.command != spec.command) {
            return Err(io::Error::other("MCP declaration changed; refresh its tool binding before reconnecting"));
        }
        if owners.get(&spec.scope).is_none_or(|owner| owner.events.is_closed()) {
            let (events, requests) = mpsc::channel(64);
            let initial = spec.clone();
            let server_name = initial.scope.name.clone();
            tokio::spawn(async move {
                if let Err(error) = run_owner(initial, requests).await {
                    tracing::warn!(server = server_name, %error, "shared MCP owner stopped");
                }
            });
            owners.insert(spec.scope.clone(), Owner { command: spec.command.clone(), events });
        }
        let owner = owners.get_mut(&spec.scope).expect("shared MCP owner was inserted");
        owner.command = spec.command.clone();
        owner.events.clone()
    };
    let client = CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let protocol_mode = spec.command.protocol_mode();
    let max_line_bytes = stdio_line_limit(protocol_mode);
    let (stream, peer) = tokio::io::duplex(64 * 1024);
    let (output, messages) = mpsc::channel(32);
    let (ready, wait) = oneshot::channel();
    events.send(Event::Attach(client, spec, output, ready, reconnect)).await.map_err(io::Error::other)?;
    // Start cleanup before awaiting attachment. Cancelling startup closes this
    // stream and releases the peer rather than leaving an immortal owner lease.
    tokio::spawn(async move {
        if let Err(error) = serve_peer(client, peer, events, messages, max_line_bytes).await {
            tracing::warn!(%error, "shared MCP peer closed");
        }
    });
    tokio::time::timeout(START_TIMEOUT, wait).await.map_err(io::Error::other)?
        .map_err(|_| io::Error::other("shared MCP owner exited during startup"))?
        .map_err(io::Error::other)?;
    Ok(StdioServerTransport::shared(stream, max_line_bytes, protocol_mode))
}

async fn launch(spec: &ServerSpec) -> io::Result<StdioServerTransport> {
    let command = spec.command.clone();
    let cwd = spec.scope.cwd.clone();
    tokio::task::spawn_blocking(move || LocalStdioServerLauncher::launch_server(command, cwd)).await.map_err(io::Error::other)?
}

enum Event {
    Attach(u64, ServerSpec, mpsc::Sender<Value>, oneshot::Sender<Result<(), String>>, bool),
    Message(u64, Value),
    Disconnect(u64),
}

struct Pending {
    client: u64,
    original_id: Value,
    upstream_id: Value,
    original_progress: Option<Value>,
    progress: Value,
    message: Value,
    started: Instant,
    timeout: Duration,
    cancelled_at: Option<Instant>,
}

impl Pending {
    fn deadline(&self) -> Instant {
        self.cancelled_at.map(|at| at + Duration::from_secs(5))
            .unwrap_or(self.started + self.timeout.max(START_TIMEOUT))
    }
}

struct Peer {
    output: mpsc::Sender<Value>,
    request_timeout: Duration,
}

/// Drive writes independently of reads: a server can emit notifications while
/// consuming a large tool argument. Awaiting stdin in the reader loop deadlocks
/// when both OS pipes fill. Keep a bounded, ordered queue of send futures.
struct OwnedTransport {
    transport: StdioServerTransport,
    writes: mpsc::Sender<BoxFuture<'static, io::Result<()>>>,
    writer: tokio::task::JoinHandle<io::Result<()>>,
}

impl OwnedTransport {
    fn new(transport: StdioServerTransport) -> Self {
        let (writes, mut pending) = mpsc::channel::<BoxFuture<'static, io::Result<()>>>(256);
        let writer = tokio::spawn(async move {
            while let Some(write) = pending.recv().await {
                write.await?;
            }
            Ok(())
        });
        Self { transport, writes, writer }
    }

    fn send(&mut self, message: Value) -> io::Result<()> {
        let message = serde_json::from_value(message).map_err(io::Error::other)?;
        self.writes.try_send(self.transport.send(message).boxed()).map_err(|_| {
            io::Error::new(io::ErrorKind::WouldBlock, "shared MCP writer queue is full or closed")
        })
    }
}

impl Drop for OwnedTransport {
    fn drop(&mut self) {
        self.writer.abort();
    }
}

struct SharedServer {
    peers: HashMap<u64, Peer>,
    queue: VecDeque<Pending>,
    active: HashMap<String, Pending>,
    callbacks: HashMap<String, (u64, Value)>,
    initialize: Option<(Value, Value)>,
    initialized: bool,
    sequence: u64,
    legacy: bool,
}

impl SharedServer {
    fn new(protocol_mode: McpProtocolMode) -> Self {
        Self { peers: HashMap::new(), queue: VecDeque::new(), active: HashMap::new(), callbacks: HashMap::new(), initialize: None, initialized: false, sequence: 0, legacy: protocol_mode == McpProtocolMode::Legacy }
    }

    fn next_id(&mut self) -> Value {
        self.sequence += 1;
        Value::String(format!("codex-shared-{}", self.sequence))
    }

    fn reply(&mut self, client: u64, message: Value) {
        if self.peers.get(&client).is_some_and(|peer| peer.output.try_send(message).is_err()) {
            self.peers.remove(&client);
        }
    }

    async fn send(server: &mut OwnedTransport, message: Value) -> io::Result<()> {
        server.send(message)
    }

    async fn from_client(&mut self, client: u64, mut message: Value, server: &mut OwnedTransport) -> io::Result<()> {
        let Some(peer) = self.peers.get(&client) else { return Ok(()); };
        let timeout = peer.request_timeout;
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            if let Some(id) = message.get("id").and_then(Value::as_str)
                && let Some((owner, original_id)) = self.callbacks.get(id)
                && *owner == client
            {
                let original_id = original_id.clone();
                self.callbacks.remove(id);
                message["id"] = original_id;
                Self::send(server, message).await?;
            }
            return Ok(());
        };
        if method == "notifications/initialized" {
            if self.initialized { return Ok(()); }
            self.initialized = true;
        }
        if method == "initialize"
            && let Some(reply) = self.initialize_reply(&message)
        {
            self.reply(client, reply);
            return Ok(());
        }
        if message.get("id").is_none() {
            if method == "notifications/cancelled" {
                let id = message.pointer("/params/requestId").cloned().unwrap_or(Value::Null);
                self.queue.retain(|pending| pending.client != client || pending.original_id != id);
                let Some(active) = self.active.values_mut().find(|active| active.client == client && active.original_id == id) else { return Ok(()); };
                active.cancelled_at.get_or_insert_with(Instant::now);
                message["params"]["requestId"] = active.upstream_id.clone();
                if !self.legacy {
                    self.active.remove(message["params"]["requestId"].as_str().unwrap_or_default());
                }
            }
            return Self::send(server, message).await;
        }
        let original_id = message["id"].clone();
        if self.queue.len() >= 128 {
            self.reply(client, rpc_error(original_id, "shared MCP request queue is full"));
            return Ok(());
        }
        let upstream_id = self.next_id();
        let original_progress = message.pointer("/params/_meta/progressToken").cloned();
        let progress = self.next_id();
        if original_progress.is_some() { message["params"]["_meta"]["progressToken"] = progress.clone(); }
        message["id"] = upstream_id.clone();
        self.queue.push_back(Pending { client, original_id, upstream_id, original_progress, progress, message, started: Instant::now(), timeout, cancelled_at: None });
        Ok(())
    }

    fn initialize_reply(&self, message: &Value) -> Option<Value> {
        let (params, result) = self.initialize.as_ref()?;
        let incoming = &message["params"];
        let capabilities = sharing_capabilities(&params["capabilities"], result);
        let incoming_capabilities = sharing_capabilities(&incoming["capabilities"], result);
        Some(if incoming_capabilities == capabilities && incoming["protocolVersion"] == params["protocolVersion"] {
            json!({"jsonrpc":"2.0", "id":message["id"], "result":result})
        } else {
            let mut error = rpc_error(message["id"].clone(), "shared MCP clients require compatible protocol versions and capabilities");
            let fields = capabilities.as_object().into_iter().flat_map(|value| value.keys())
                .chain(incoming_capabilities.as_object().into_iter().flat_map(|value| value.keys()))
                .filter(|key| capabilities.get(*key) != incoming_capabilities.get(*key))
                .collect::<std::collections::BTreeSet<_>>();
            error["error"]["data"] = json!({
                "protocolVersionMatches": incoming["protocolVersion"] == params["protocolVersion"],
                "differentCapabilityFields": fields,
            });
            error
        })
    }

    async fn dispatch(&mut self, server: &mut OwnedTransport) -> io::Result<()> {
        while (!self.legacy || self.active.is_empty()) && self.active.len() < 128 {
            let Some(mut pending) = self.queue.pop_front() else { break; };
            if !self.peers.contains_key(&pending.client) { continue; }
            if pending.message["method"] == "initialize"
                && let Some(mut reply) = self.initialize_reply(&pending.message)
            {
                reply["id"] = pending.original_id;
                self.reply(pending.client, reply);
                continue;
            }
            pending.started = Instant::now();
            Self::send(server, pending.message.clone()).await?;
            self.active.insert(pending.upstream_id.as_str().unwrap_or_default().to_owned(), pending);
        }
        Ok(())
    }

    async fn from_server(&mut self, mut message: Value, server: &mut OwnedTransport) -> io::Result<()> {
        if message.get("method").is_none() {
            let Some(id) = message.get("id").and_then(Value::as_str) else { return Ok(()); };
            let Some(active) = self.active.remove(id) else { return Ok(()); };
            if active.message["method"] == "initialize" && message.get("result").is_some() {
                self.initialize = Some((active.message["params"].clone(), message["result"].clone()));
            }
            message["id"] = active.original_id;
            self.reply(active.client, message);
            return Ok(());
        }
        if message.get("id").is_some() {
            if message["method"] == "ping" {
                return Self::send(server, json!({"jsonrpc":"2.0", "id":message["id"], "result":{}})).await;
            }
            let original_id = message["id"].clone();
            let parent = message.pointer("/params/_meta/relatedRequestId").and_then(Value::as_str);
            let progress = message.pointer("/params/_meta/progressToken");
            let active = parent.and_then(|id| self.active.get(id))
                .or_else(|| self.active.values().find(|active| progress == Some(&active.progress)))
                .or_else(|| (parent.is_none() && progress.is_none() && self.legacy && self.active.len() == 1).then(|| self.active.values().next()).flatten());
            let Some(active) = active.filter(|active| active.cancelled_at.is_none() && self.peers.contains_key(&active.client)) else {
                return Self::send(server, rpc_error(original_id, "no calling conversation for this MCP request")).await;
            };
            let owner = active.client;
            let original_request = active.original_id.clone();
            let upstream_request = active.upstream_id.clone();
            let alias = self.next_id();
            self.callbacks.insert(alias.as_str().unwrap_or_default().to_owned(), (owner, original_id));
            message["id"] = alias;
            if message.pointer("/params/_meta/relatedRequestId") == Some(&upstream_request) {
                message["params"]["_meta"]["relatedRequestId"] = original_request;
            }
            self.restore_progress(&mut message);
            self.reply(owner, message);
        } else if message["method"] == "notifications/cancelled" {
            let request_id = message.pointer("/params/requestId").cloned().unwrap_or(Value::Null);
            if let Some((alias, (owner, _))) = self.callbacks.iter().find(|(_, (_, id))| *id == request_id) {
                let owner = *owner;
                message["params"]["requestId"] = Value::String(alias.clone());
                self.reply(owner, message);
            } else if let Some(active) = self.active.values().find(|active| active.upstream_id == request_id) {
                let owner = active.client;
                message["params"]["requestId"] = active.original_id.clone();
                self.reply(owner, message);
            }
        } else if message["method"] == "notifications/progress" {
            let token = message.pointer("/params/progressToken");
            let active = self.active.values().find(|active| token == Some(&active.progress));
            let Some(active) = active else { return Ok(()); };
            let owner = active.client;
            if message.pointer("/params/progressToken") == Some(&active.progress) {
                message["params"]["progressToken"] = active.original_progress.clone().unwrap_or(Value::Null);
            }
            self.reply(owner, message);
        } else {
            // Tool/resource catalog changes and resource updates describe the
            // shared service. Each RMCP client retains its own subscriptions.
            for client in self.peers.keys().copied().collect::<Vec<_>>() { self.reply(client, message.clone()); }
        }
        Ok(())
    }

    fn restore_progress(&self, message: &mut Value) {
        if let Some(active) = self.active.values().find(|active| message.pointer("/params/_meta/progressToken") == Some(&active.progress))
        {
            message["params"]["_meta"]["progressToken"] = active.original_progress.clone().unwrap_or(Value::Null);
        }
    }

    async fn disconnect(&mut self, client: u64, server: &mut OwnedTransport) -> io::Result<()> {
        self.peers.remove(&client);
        self.queue.retain(|pending| pending.client != client);
        let abandoned = self.callbacks.iter().filter(|(_, (owner, _))| *owner == client).map(|(alias, (_, id))| (alias.clone(), id.clone())).collect::<Vec<_>>();
        for (alias, id) in abandoned {
            self.callbacks.remove(&alias);
            Self::send(server, rpc_error(id, "calling MCP conversation disconnected")).await?;
        }
        for id in self.active.values().filter(|active| active.client == client).map(|active| active.upstream_id.clone()).collect::<Vec<_>>() {
            Self::send(server, json!({"jsonrpc":"2.0", "method":"notifications/cancelled", "params":{"requestId":id, "reason":"calling conversation disconnected"}})).await?;
        }
        if self.legacy {
            for active in self.active.values_mut().filter(|active| active.client == client) {
                active.cancelled_at.get_or_insert_with(Instant::now);
            }
        } else {
            self.active.retain(|_, active| active.client != client);
        }
        Ok(())
    }

    async fn expire_requests(&mut self, server: &mut OwnedTransport) -> io::Result<()> {
        let now = Instant::now();
        let expired = self.active.iter().filter(|(_, active)| active.deadline() <= now)
            .map(|(id, _)| id.clone()).collect::<Vec<_>>();
        for id in expired {
            let Some(mut active) = self.active.remove(&id) else { continue; };
            if self.legacy && active.cancelled_at.is_some() {
                // A legacy server that never completes cancellation cannot be
                // reused safely: its unlabelled callbacks could target the next caller.
                return Err(io::Error::new(io::ErrorKind::TimedOut, "legacy MCP server did not finish a cancelled request"));
            }
            Self::send(server, json!({"jsonrpc":"2.0", "method":"notifications/cancelled", "params":{"requestId":active.upstream_id, "reason":"request timed out"}})).await?;
            self.reply(active.client, rpc_error(active.original_id.clone(), "MCP request timed out"));
            if self.legacy {
                active.cancelled_at = Some(now);
                self.active.insert(id, active);
            }
        }
        Ok(())
    }
}

fn sharing_capabilities(capabilities: &Value, initialize_result: &Value) -> Value {
    let mut capabilities = capabilities.clone();
    // Chat clients own an AuthManager; standalone status/resource discovery
    // does not. Auth invalidations are sent only after the server opts into
    // this extension (codex-mcp/auth_changes.rs). An unaccepted offer cannot
    // affect that shared connection, regardless of which client started it.
    // Keep the difference significant for servers that actually use it.
    if initialize_result.pointer("/capabilities/experimental/codex~1auth-change").is_none()
        && let Some(experimental) = capabilities.get_mut("experimental").and_then(Value::as_object_mut)
    {
        experimental.remove("codex/auth-change");
        if experimental.is_empty()
            && let Some(capabilities) = capabilities.as_object_mut()
        {
            capabilities.remove("experimental");
        }
    }
    capabilities
}

fn rpc_error(id: Value, message: &str) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32603, "message":message}})
}

async fn run_owner(mut spec: ServerSpec, mut requests: mpsc::Receiver<Event>) -> io::Result<()> {
    let mut server = match launch(&spec).await {
        Ok(server) => OwnedTransport::new(server),
        Err(error) => {
            requests.close();
            while let Some(event) = requests.recv().await {
                if let Event::Attach(_, _, _, ready, _) = event {
                    let _ = ready.send(Err(error.to_string()));
                }
            }
            return Err(error);
        }
    };
    let mut owner = SharedServer::new(spec.command.protocol_mode());
    let mut idle_since = Instant::now();
    let outcome = async {
        loop {
            let expiry = owner.active.values().map(Pending::deadline).min();
            tokio::select! {
                event = requests.recv() => {
                    let Some(event) = event else { return Ok(()); };
                    match event {
                        Event::Attach(client, incoming, output, result, reconnect) => {
                            let request_timeout = incoming.request_timeout;
                            if incoming.command != spec.command {
                                if reconnect {
                                    let _ = result.send(Err("MCP declaration changed; refresh its tool binding before reconnecting".to_owned()));
                                    continue;
                                }
                                // Replace a changed declaration in its owning slot.
                                // Drop old clients before retiring their exact process.
                                owner = SharedServer::new(incoming.command.protocol_mode());
                                close_server(&mut server).await?;
                                spec = incoming;
                                server = OwnedTransport::new(launch(&spec).await?);
                            }
                            owner.peers.insert(client, Peer { output, request_timeout });
                            let _ = result.send(Ok(()));
                        }
                        Event::Message(client, message) => owner.from_client(client, message, &mut server).await?,
                        Event::Disconnect(client) => owner.disconnect(client, &mut server).await?,
                    }
                    if !owner.peers.is_empty() { idle_since = Instant::now(); }
                }
                message = server.transport.receive() => {
                    let Some(message) = message else { return Ok(()); };
                    owner.from_server(serde_json::to_value(message).map_err(io::Error::other)?, &mut server).await?;
                }
                result = &mut server.writer => {
                    return result.map_err(io::Error::other)?;
                }
                _ = tokio::time::sleep_until(expiry.unwrap_or_else(|| Instant::now() + IDLE_TIMEOUT)), if expiry.is_some() => {
                    owner.expire_requests(&mut server).await?;
                }
                _ = tokio::time::sleep_until(idle_since + IDLE_TIMEOUT), if owner.peers.is_empty() => return Ok(()),
            }
            owner.dispatch(&mut server).await?;
        }
    }.await;
    let cleanup = close_server(&mut server).await;
    // Keep the receiver open until process-group cleanup finishes. A racing
    // acquire must not start another owner while the old child is terminating.
    requests.close();
    // Expose the closed peer only after its owner can be safely reacquired.
    owner.peers.clear();
    while let Some(event) = requests.recv().await {
        if let Event::Attach(_, _, _, ready, _) = event {
            let _ = ready.send(Err("shared MCP owner closed; retry attachment".to_owned()));
        }
    }
    outcome.and(cleanup)
}

async fn close_server(server: &mut OwnedTransport) -> io::Result<()> {
    server.writer.abort();
    let result = server.transport.close().await;
    // The existing launcher escalates process-group termination after two
    // seconds. Retain ownership until descendants have received that signal.
    #[cfg(unix)]
    tokio::time::sleep(Duration::from_secs(2)).await;
    result
}

async fn serve_peer(client: u64, stream: DuplexStream, events: mpsc::Sender<Event>, mut messages: mpsc::Receiver<Value>, max_line_bytes: usize) -> io::Result<()> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let outcome = async {
        let incoming = async {
            loop {
                let Some(message) = read_message(&mut reader, max_line_bytes).await? else { return Ok::<(), io::Error>(()); };
                events.send(Event::Message(client, message)).await.map_err(io::Error::other)?;
            }
        };
        tokio::pin!(incoming);
        loop {
            tokio::select! {
                message = messages.recv() => {
                    let Some(message) = message else { return Ok(()); };
                    write_message(&mut writer, &message, max_line_bytes).await?;
                }
                // Preserve partial input while outgoing notifications arrive.
                result = &mut incoming => return result,
            }
        }
    }.await;
    let _ = events.send(Event::Disconnect(client)).await;
    outcome
}

async fn read_message(reader: &mut (impl AsyncBufRead + Unpin), max_line_bytes: usize) -> io::Result<Option<Value>> {
    let mut bytes = Vec::new();
    let read = reader.take((max_line_bytes + 2) as u64).read_until(b'\n', &mut bytes).await?;
    if read == 0 { return Ok(None); }
    let line = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
    if line.len() > max_line_bytes { return Err(io::Error::new(io::ErrorKind::InvalidData, "MCP frame exceeds the stdio message limit")); }
    serde_json::from_slice(&bytes).map(Some).map_err(io::Error::other)
}

async fn write_message(writer: &mut (impl AsyncWrite + Unpin), message: &Value, max_line_bytes: usize) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(message).map_err(io::Error::other)?;
    if bytes.len() > max_line_bytes { return Err(io::Error::new(io::ErrorKind::InvalidData, "MCP frame exceeds the stdio message limit")); }
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}
