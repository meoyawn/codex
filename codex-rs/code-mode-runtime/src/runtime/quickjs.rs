//! QuickJS engine adapter. Session/cell ownership remains in the upstream actors.
#[path = "audio.rs"]
mod audio;

use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use codex_code_mode_protocol::{CodeModeToolKind, EnabledToolMetadata, ExecuteRequest,
    FunctionCallOutputContentItem, normalize_code_mode_identifier};
use codex_protocol::ToolName;
use rquickjs::{CaughtError, Context, Ctx, Exception, Function, Module, Object,
    Persistent, Promise, Runtime, Value, FromJs};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use crate::TaskFailureHandler;

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    ToolResponse { id: String, result: JsonValue },
    ToolError { id: String, error_text: String },
    TimeoutFired { id: u64 },
    ObservePendingFrontier,
    Terminate,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum PendingRuntimeMode {
    #[cfg(test)]
    Continue,
    PauseUntilResumed,
}

#[derive(Debug)]
pub(crate) enum RuntimeControlCommand {
    Continue,
    Resume,
    Terminate,
}

#[derive(Debug)]
pub(crate) enum RuntimeEvent {
    Started,
    Pending,
    ContentItem(FunctionCallOutputContentItem),
    YieldRequested,
    ToolCall {
        id: String,
        name: ToolName,
        kind: CodeModeToolKind,
        input: Option<JsonValue>,
    },
    Notify {
        call_id: String,
        text: String,
    },
    Result {
        stored_value_writes: HashMap<String, Arc<JsonValue>>,
        error_text: Option<String>,
    },
    ThreadPanicked,
}

#[derive(Clone, Default)]
pub(crate) struct RuntimeTerminateHandle(Arc<AtomicBool>);

impl RuntimeTerminateHandle {
    pub(crate) fn terminate_execution(&self) -> bool {
        self.0.store(true, Ordering::Release);
        true
    }
}

struct RuntimeState {
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    stored_values: HashMap<String, Arc<JsonValue>>,
    stored_value_writes: HashMap<String, Arc<JsonValue>>,
    enabled_tools: Vec<EnabledToolMetadata>,
    next_tool_call_id: u64,
    tool_call_id: String,
    command_tx: std_mpsc::Sender<RuntimeCommand>,
    timers: HashMap<u64, AbortOnDropHandle<()>>,
    termination: RuntimeTerminateHandle,
    exited: bool,
}

pub(crate) fn spawn_runtime(
    stored_values: HashMap<String, Arc<JsonValue>>,
    request: ExecuteRequest,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    pending_mode: PendingRuntimeMode,
    task_failure_handler: Option<TaskFailureHandler>,
) -> Result<(std_mpsc::Sender<RuntimeCommand>, std_mpsc::Sender<RuntimeControlCommand>, RuntimeTerminateHandle), String> {
    let (command_tx, command_rx) = std_mpsc::channel();
    let (control_tx, control_rx) = std_mpsc::channel();
    let termination = RuntimeTerminateHandle::default();
    let runtime_termination = termination.clone();
    let runtime_command_tx = command_tx.clone();
    let runtime_handle = tokio::runtime::Handle::current();
    std::thread::Builder::new().name("codex-quickjs-cell".into()).spawn(move || {
        let events = event_tx.clone();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = runtime_handle.enter();
            let enabled_tools = request.enabled_tools.into_iter().map(|definition| EnabledToolMetadata {
                global_name: normalize_code_mode_identifier(&definition.name),
                tool_name: definition.tool_name,
                description: definition.description,
                kind: definition.kind,
            }).collect();
            let state = Rc::new(RefCell::new(RuntimeState {
                event_tx, stored_values, stored_value_writes: HashMap::new(), enabled_tools,
                next_tool_call_id: 1, tool_call_id: request.tool_call_id,
                command_tx: runtime_command_tx, timers: HashMap::new(),
                termination: runtime_termination.clone(), exited: false,
            }));
            let error_text = run_runtime(&state, &request.source, &command_rx, &control_rx, pending_mode).err();
            let state = state.borrow();
            let _ = state.event_tx.send(RuntimeEvent::Result {
                stored_value_writes: state.stored_value_writes.clone(),
                error_text: if state.exited { None } else { error_text },
            });
        }));
        if result.is_err() {
            if let Some(handler) = task_failure_handler {
                handler("code-mode QuickJS runtime thread panicked".into());
            }
            let _ = events.send(RuntimeEvent::ThreadPanicked);
        }
    }).map_err(|error| format!("failed to start code-mode runtime thread: {error}"))?;
    Ok((command_tx, control_tx, termination))
}

fn error_text(ctx: &Ctx<'_>, error: rquickjs::Error) -> String {
    match CaughtError::from_error(ctx, error) {
        CaughtError::Value(value) => {
            rquickjs::Coerced::<String>::from_js(ctx, value)
                .map(|value| value.0).unwrap_or_else(|_| "unknown code mode exception".into())
        }
        error => error.to_string(),
    }
}

fn parse_json(value: &str) -> Result<JsonValue, String> {
    // Keep upstream's serde private-key guard before deserialization.
    for key in [r#"{"$serde_json::private::RawValue":"#, r#","$serde_json::private::RawValue":"#,
        r#"{"$serde_json::private::Number":"#, r#","$serde_json::private::Number":"#] {
        if value.contains(key) { return Err("failed to serialize JavaScript value: reserved JSON object key".into()); }
    }
    serde_json::from_str(value).map_err(|error| format!("failed to serialize JavaScript value: {error}"))
}

fn host_call(state: &Rc<RefCell<RuntimeState>>, kind: &str, payload: &str) -> Result<String, String> {
    let payload = parse_json(payload)?;
    let mut state = state.borrow_mut();
    if state.termination.0.load(Ordering::Acquire) && kind != "exit" {
        return Err("code mode execution terminated".into());
    }
    let mut response = JsonValue::Null;
    match kind {
        "output" => {
            let mut item: FunctionCallOutputContentItem = serde_json::from_value(payload)
                .map_err(|error| format!("invalid code-mode output: {error}"))?;
            if let FunctionCallOutputContentItem::InputAudio { audio_url } = &item {
                if audio::wav_duration_seconds(audio_url).is_some_and(|seconds| seconds < 0.025) {
                    item = FunctionCallOutputContentItem::InputText {
                        text: "Audio output omitted because the clip is shorter than 25 ms; use a longer clip.".into(),
                    };
                }
            }
            let _ = state.event_tx.send(RuntimeEvent::ContentItem(item));
        }
        "tool" => {
            let index = payload["index"].as_u64().ok_or("invalid tool callback data")? as usize;
            let tool = state.enabled_tools.get(index).ok_or("tool callback data is out of range")?;
            let name = tool.tool_name.clone();
            let kind = tool.kind;
            let id = format!("tool-{}", state.next_tool_call_id);
            state.next_tool_call_id = state.next_tool_call_id.saturating_add(1);
            let input = payload.get("input").cloned();
            let _ = state.event_tx.send(RuntimeEvent::ToolCall { id: id.clone(), name, kind, input });
            response = JsonValue::String(id);
        }
        "store" => {
            let key = payload["key"].as_str().ok_or("store key must be a string")?.to_string();
            let value = Arc::new(payload.get("value").cloned().ok_or("Only plain serializable objects can be stored.")?);
            state.stored_values.insert(key.clone(), value.clone());
            state.stored_value_writes.insert(key, value);
        }
        "load" => {
            let key = payload.as_str().ok_or("load key must be a string")?;
            response = match state.stored_values.get(key) {
                Some(value) => serde_json::json!({"present": true, "value": value}),
                None => serde_json::json!({"present": false}),
            };
        }
        "notify" => {
            let text = payload.as_str().filter(|text| !text.trim().is_empty()).ok_or("notify expects non-empty text")?.to_string();
            let _ = state.event_tx.send(RuntimeEvent::Notify { call_id: state.tool_call_id.clone(), text });
        }
        "yield" => { let _ = state.event_tx.send(RuntimeEvent::YieldRequested); }
        "exit" => { state.exited = true; state.termination.terminate_execution(); }
        "setTimeout" => {
            let id = payload["id"].as_u64().ok_or("invalid timer id")?;
            let delay = payload["delay"].as_u64().unwrap_or(0);
            let tx = state.command_tx.clone();
            let sleep = tokio::time::sleep(Duration::from_millis(delay));
            state.timers.insert(id, AbortOnDropHandle::new(tokio::spawn(async move {
                sleep.await;
                let _ = tx.send(RuntimeCommand::TimeoutFired { id });
            })));
        }
        "clearTimeout" => {
            if let Some(id) = payload.as_u64() { state.timers.remove(&id); }
        }
        _ => return Err("unknown code-mode callback".into()),
    }
    Ok(response.to_string())
}

fn run_runtime(
    state: &Rc<RefCell<RuntimeState>>, source: &str,
    command_rx: &std_mpsc::Receiver<RuntimeCommand>,
    control_rx: &std_mpsc::Receiver<RuntimeControlCommand>, pending_mode: PendingRuntimeMode,
) -> Result<(), String> {
    let runtime = Runtime::new().map_err(|error| error.to_string())?;
    let interrupt = state.borrow().termination.clone();
    runtime.set_interrupt_handler(Some(Box::new(move || interrupt.0.load(Ordering::Acquire))));
    let context = Context::full(&runtime).map_err(|error| error.to_string())?;
    let controller: Persistent<Object<'static>> = context.with(|ctx| {
        let callback_state = state.clone();
        let callback = Function::new(ctx.clone(), move |ctx: Ctx<'_>, kind: String, payload: String| {
            host_call(&callback_state, &kind, &payload)
                .map_err(|message| Exception::throw_type(&ctx, &message))
        })?;
        ctx.globals().set("__codexHost", callback)?;
        let tools = state.borrow().enabled_tools.iter().map(|tool|
            serde_json::json!({"name": tool.global_name, "description": tool.description})).collect::<Vec<_>>();
        ctx.globals().set("__codexTools", ctx.json_parse(serde_json::to_vec(&tools).map_err(|_| rquickjs::Error::Unknown)?)?)?;
        let controller = ctx.eval::<Object, _>(include_str!("quickjs_globals.js"))?;
        ctx.eval::<(), _>(include_str!("settled.js"))?;
        Ok::<_, rquickjs::Error>(Persistent::save(&ctx, controller))
    }).map_err(|error| context.with(|ctx| error_text(&ctx, error)))?;
    let events = state.borrow().event_tx.clone();
    let _ = events.send(RuntimeEvent::Started);
    let promise: Persistent<Promise<'static>> = context.with(|ctx| {
        Module::evaluate(ctx.clone(), "exec_main.mjs", source)
            .map(|promise| Persistent::save(&ctx, promise))
            .map_err(|error| error_text(&ctx, error))
    })?;
    loop {
        while runtime.execute_pending_job().map_err(|error| error.0.with(|ctx| error_text(&ctx, rquickjs::Error::Exception)))? {
            if state.borrow().termination.0.load(Ordering::Acquire) { break; }
        }
        if state.borrow().termination.0.load(Ordering::Acquire) {
            return if state.borrow().exited { Ok(()) } else { Err("code mode execution terminated".into()) };
        }
        let result = context.with(|ctx| {
            let promise = promise.clone().restore(&ctx).map_err(|error| error_text(&ctx, error))?;
            Ok::<_, String>(promise.result::<Value>().map(|result| result.map(|_| ()).map_err(|error| error_text(&ctx, error))))
        })?;
        if let Some(result) = result { return result; }
        let Some(command) = next_runtime_command(&events, command_rx, control_rx, pending_mode) else { return Ok(()); };
        let (method, arguments) = match command {
            RuntimeCommand::Terminate => return Ok(()),
            RuntimeCommand::ObservePendingFrontier => continue,
            RuntimeCommand::ToolResponse { id, result } => ("resolve", serde_json::json!([id, true, result])),
            RuntimeCommand::ToolError { id, error_text } => ("resolve", serde_json::json!([id, false, error_text])),
            RuntimeCommand::TimeoutFired { id } => {
                state.borrow_mut().timers.remove(&id);
                ("timeout", serde_json::json!([id]))
            }
        };
        context.with(|ctx| {
            let controller = controller.clone().restore(&ctx)?;
            let function: Function = controller.get(method)?;
            function.call::<_, ()>((arguments.to_string(),))
                .map_err(|error| error_text(&ctx, error))
                .map_err(|message| Exception::throw_message(&ctx, &message))
        }).map_err(|error| context.with(|ctx| error_text(&ctx, error)))?;
    }
}

fn next_runtime_command(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    command_rx: &std_mpsc::Receiver<RuntimeCommand>,
    control_rx: &std_mpsc::Receiver<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
) -> Option<RuntimeCommand> {
    loop {
        match command_rx.try_recv() {
            Ok(command) => return Some(command),
            Err(std_mpsc::TryRecvError::Disconnected) => return None,
            Err(std_mpsc::TryRecvError::Empty) => {}
        }

        let _ = event_tx.send(RuntimeEvent::Pending);
        match pending_mode {
            #[cfg(test)]
            PendingRuntimeMode::Continue => return command_rx.recv().ok(),
            PendingRuntimeMode::PauseUntilResumed => match control_rx.recv().ok()? {
                RuntimeControlCommand::Continue => return command_rx.recv().ok(),
                RuntimeControlCommand::Resume => continue,
                RuntimeControlCommand::Terminate => return Some(RuntimeCommand::Terminate),
            },
        }
    }
}

