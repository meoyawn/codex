//! Desktop-owned MCP over RMCP's native in-process transport. There is no
//! subprocess, JavaScript worker, socket listener, or model/auth HTTP change.
use super::*;
use codex_protocol::dynamic_tools::{DynamicToolCallRequest, DynamicToolNamespaceTool, DynamicToolSpec};
use codex_rmcp_client::InProcessTransportFactory;
use rmcp::ErrorData as McpError;
use rmcp::ServiceExt;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{CallToolRequestParams, CallToolResponse, Implementation, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::{Value, json};
use std::sync::Weak;
use tokio::sync::oneshot;

#[derive(Clone)]
struct AppTools {
    session: Weak<Session>,
    tools: Vec<Tool>,
}

impl InProcessTransportFactory for AppTools {
    fn open(&self) -> futures::future::BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let server = self.clone();
        Box::pin(async move {
            let (client, stream) = tokio::io::duplex(64 * 1024);
            let (reader, writer) = tokio::io::split(stream);
            tokio::spawn(async move {
                match server.serve((reader, writer)).await {
                    Ok(service) => { let _ = service.waiting().await; }
                    Err(error) => tracing::debug!(%error, "app MCP transport retired"),
                }
            });
            Ok(client)
        })
    }
}

fn failure(message: impl Into<String>) -> McpError { McpError::internal_error(message.into(), None) }
fn text_meta(meta: &Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| meta[*name].as_str().filter(|text| !text.trim().is_empty()).map(str::to_string))
}

impl ServerHandler for AppTools {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("codex-app-tools", "0.1.0").with_title("ChatGPT App Tools"))
            .with_instructions("Manage the Codex desktop app: find, create, and update chats; organize projects, sidebar, and pinned items; schedule automations; manage worktrees; open app panels; and locate bundled libraries for documents, spreadsheets, slides, and PDFs.")
    }
    async fn list_tools(&self, _request: Option<PaginatedRequestParams>, _context: RequestContext<RoleServer>) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(self.tools.clone()))
    }
    async fn call_tool(&self, request: CallToolRequestParams, context: RequestContext<RoleServer>) -> Result<CallToolResponse, McpError> {
        if !self.tools.iter().any(|tool| tool.name == request.name) {
            return Err(McpError::invalid_params(format!("Unknown Codex app tool: {}", request.name), None));
        }
        let session = self.session.upgrade().ok_or_else(|| failure("The app MCP session ended"))?;
        let meta = serde_json::to_value(&context.meta).map_err(|error| failure(error.to_string()))?;
        let turn_meta = match &meta["x-codex-turn-metadata"] {
            Value::String(text) => serde_json::from_str(text).unwrap_or(Value::Null), value => value.clone(),
        };
        let source_thread = text_meta(&meta, &["openai/threadId","openai/thread_id","codexThreadId","codex_thread_id","threadId","thread_id"])
            .or_else(|| text_meta(&turn_meta, &["thread_id"]))
            .or_else(|| text_meta(&meta["thread"], &["id"]))
            .ok_or_else(|| McpError::invalid_params("Codex app tools require thread metadata from the executor.", None))?;
        if source_thread != session.session_id().to_string() { return Err(failure("The app tool caller does not own this session")); }
        let call_id = text_meta(&meta, &["openai/toolCallId","openai/tool_call_id","codexCallId","codex_call_id","callId","call_id"])
            .or_else(|| text_meta(&meta["call"], &["id"]))
            .unwrap_or_else(|| format!("mcp-call-{}", uuid::Uuid::new_v4()));
        let metadata_turn = text_meta(&meta, &["openai/turnId","openai/turn_id","codexTurnId","codex_turn_id","turnId","turn_id"])
            .or_else(|| text_meta(&turn_meta, &["turn_id"]))
            .or_else(|| text_meta(&meta["turn"], &["id"]));
        let (sender, receiver) = oneshot::channel();
        let (turn_id, turn_state, cancellation) = {
            let active = session.active_turn.lock().await;
            let active = active.as_ref().ok_or_else(|| failure("The calling turn ended"))?;
            let task = active.task.as_ref().ok_or_else(|| failure("The calling turn ended"))?;
            let turn_id = task.turn_context.sub_id.clone();
            if context.ct.is_cancelled() || metadata_turn.as_ref().is_some_and(|id| id != &turn_id) { return Err(failure("The calling turn ended")); }
            let mut state = active.turn_state.lock().await;
            if let Some(previous) = state.insert_pending_dynamic_tool(call_id.clone(), sender) {
                state.insert_pending_dynamic_tool(call_id.clone(), previous);
                return Err(failure("The app tool call is already executing"));
            }
            (turn_id, Arc::clone(&active.turn_state), task.cancellation_token.clone())
        };
        session.send_event_raw(Event { id: turn_id.clone(), msg: EventMsg::DynamicToolCallRequest(DynamicToolCallRequest {
            call_id: call_id.clone(), turn_id, started_at_ms: chrono::Utc::now().timestamp_millis(),
            namespace: Some("codex_app".to_string()), tool: request.name.into_owned(), arguments: json!(request.arguments.unwrap_or_default()),
        }) }).await;
        let result = tokio::select! {
            result = receiver => result.map_err(|_| failure("The app tool request ended")),
            _ = context.ct.cancelled() => Err(failure("The app tool request was cancelled")),
            _ = cancellation.cancelled() => Err(failure("The calling turn ended")),
        };
        turn_state.lock().await.remove_pending_dynamic_tool(&call_id);
        let result = serde_json::to_value(result?).map_err(|error| failure(error.to_string()))?;
        let items = result["contentItems"].as_array().ok_or_else(|| failure("Invalid app tool result"))?;
        let mut content = Vec::new();
        for item in items {
            match item["type"].as_str() {
                Some("inputText") => content.push(json!({"type":"text","text":item["text"]})),
                Some("inputImage" | "inputAudio") => {
                    let image = item["type"] == "inputImage";
                    let url = item[if image {"imageUrl"} else {"audioUrl"}].as_str().ok_or_else(|| failure("Invalid app tool media"))?;
                    if let Some((mime, data)) = url.strip_prefix("data:").and_then(|text| text.split_once(";base64,")) {
                        content.push(json!({"type":if image {"image"} else {"audio"},"mimeType":mime,"data":data}));
                    } else { content.push(json!({"type":"text","text":url})); }
                }
                _ => return Err(failure("Invalid app tool content")),
            }
        }
        serde_json::from_value::<rmcp::model::CallToolResult>(json!({"content":content,"isError":result["success"] != true}))
            .map(Into::into).map_err(|error| failure(error.to_string()))
    }
}

impl Session {
    pub(super) async fn install_app_tools_mcp(self: &Arc<Self>, input: &mut codex_mcp::McpRuntimeInput) {
        let specs = self.dynamic_tools().await;
        let Some(namespace) = specs.iter().find_map(|spec| match spec {
            DynamicToolSpec::Namespace(namespace) if namespace.name == "codex_app" => Some(namespace), _ => None,
        }) else { return; };
        let mut tools = Vec::new();
        for tool in &namespace.tools {
            let DynamicToolNamespaceTool::Function(tool) = tool;
            let value = json!({"name":tool.name,"description":tool.description,"inputSchema":tool.input_schema,
                "annotations":{"destructiveHint":tool.name == "send_message_to_thread"}});
            match serde_json::from_value(value) {
                Ok(tool) => tools.push(tool),
                Err(error) => { tracing::error!(%error, "invalid app MCP specification"); return; }
            }
        }
        if tools.is_empty() { return; }
        let identity = match serde_json::to_string(namespace) { Ok(identity) => identity, Err(_) => return };
        // This is internal connection metadata, not an executable command.
        // The attached native factory is always selected before transport launch.
        let config: codex_config::McpServerConfig = match serde_json::from_value(json!({"command":"<in-process:codex_app>","enabled":true,
            "startup_timeout_sec":10,"tool_timeout_sec":3600,"default_tools_approval_mode":"approve",
            "tools":{"create_thread":{"approval_mode":"prompt"},"send_message_to_thread":{"approval_mode":"prompt"},
                "fork_thread":{"approval_mode":"prompt"},"handoff_thread":{"approval_mode":"prompt"}}})) {
            Ok(config) => config, Err(error) => { tracing::error!(%error, "invalid app MCP policy"); return; }
        };
        let runtime_config = Arc::make_mut(&mut input.config);
        let mut catalog = runtime_config.mcp_server_catalog.to_builder();
        catalog.register(codex_mcp::McpServerRegistration::from_config("codex_app".to_string(), config.clone()));
        runtime_config.mcp_server_catalog = catalog.build();
        runtime_config.server_permission_profiles.insert("codex_app".to_string(), runtime_config.permission_profile.clone());
        input.mcp_servers.insert("codex_app".to_string(), codex_mcp::EffectiveMcpServer::from_host_config(config)
            .with_in_process_transport(identity, Arc::new(AppTools {session:Arc::downgrade(self),tools})));
    }
}
