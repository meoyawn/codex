//! QuickJS adapter for the upstream bounded plugin executor. Local CodeMode
//! sessions continue to use the in-process provider, without a helper process.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use codex_plugin_host::protocol::*;
use rquickjs::{Coerced, Context, Ctx, Exception, FromJs, Function, Module, Object,
    Persistent, Promise, Runtime, Value};
use rquickjs::loader::{ImportAttributes, Loader, Resolver};
use rquickjs::promise::PromiseState;

use super::transport::Transport;

#[derive(Clone, Default)]
pub(super) struct ExecutionHandle(Arc<AtomicBool>);

impl ExecutionHandle {
    pub(super) fn terminate_execution(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Default)]
struct Failures {
    rejected: Vec<(Persistent<Value<'static>>, Persistent<Value<'static>>)>,
    overflowed: bool,
    script_error: Option<PluginScriptError>,
}

struct NoImports;

impl Resolver for NoImports {
    fn resolve<'js>(&mut self, ctx: &Ctx<'js>, _base: &str, _name: &str,
        _attributes: Option<ImportAttributes<'js>>) -> rquickjs::Result<String> {
        Err(Exception::throw_type(ctx, "module imports are unavailable"))
    }
}

impl Loader for NoImports {
    fn load<'js>(&mut self, ctx: &Ctx<'js>, _name: &str,
        _attributes: Option<ImportAttributes<'js>>) -> rquickjs::Result<Module<'js>> {
        Err(Exception::throw_type(ctx, "module imports are unavailable"))
    }
}

pub(super) struct Engine {
    // Persistent roots must be released before their context and runtime.
    namespace: Option<Persistent<Object<'static>>>,
    pending_promises: HashMap<u64, Persistent<Promise<'static>>>,
    failures: Rc<RefCell<Failures>>,
    context: Context,
    runtime: Runtime,
    termination: ExecutionHandle,
    pub(super) transport: Transport,
}

impl Engine {
    pub(super) fn new(transport: Transport) -> io::Result<Self> {
        let runtime = Runtime::new().map_err(io::Error::other)?;
        runtime.set_memory_limit(64 * 1024 * 1024);
        runtime.set_max_stack_size(512 * 1024);
        runtime.set_loader(NoImports, NoImports);
        let termination = ExecutionHandle::default();
        let interrupt = termination.clone();
        runtime.set_interrupt_handler(Some(Box::new(move || interrupt.0.load(Ordering::Acquire))));
        let context = Context::full(&runtime).map_err(io::Error::other)?;
        context.with(|ctx| ctx.eval::<(), _>(r#"
            (() => {
                const deny = function () { throw new EvalError('dynamic evaluation is unavailable'); };
                for (const fn of [Function, (async function () {}).constructor,
                    (function* () {}).constructor, (async function* () {}).constructor]) {
                    Object.defineProperty(fn.prototype, 'constructor', {
                        value: deny, writable: false, configurable: false
                    });
                }
                Object.defineProperty(globalThis, 'eval', { value: deny, writable: false, configurable: false });
                Object.defineProperty(globalThis, 'Function', { value: deny, writable: false, configurable: false });
                for (const name of ['console', 'ArrayBuffer', 'SharedArrayBuffer', 'Atomics',
                    'WebAssembly', 'DataView', 'Int8Array', 'Uint8Array', 'Uint8ClampedArray',
                    'Int16Array', 'Uint16Array', 'Int32Array', 'Uint32Array', 'Float16Array',
                    'Float32Array', 'Float64Array', 'BigInt64Array', 'BigUint64Array']) {
                    delete globalThis[name];
                }
            })();
        "#)).map_err(io::Error::other)?;
        let failures = Rc::new(RefCell::new(Failures::default()));
        let rejections = Rc::clone(&failures);
        runtime.set_host_promise_rejection_tracker(Some(Box::new(move |ctx, promise, reason, handled| {
            let promise = Persistent::save(&ctx, promise);
            let mut state = rejections.borrow_mut();
            if handled {
                state.rejected.retain(|(candidate, _)| *candidate != promise);
            } else if state.rejected.len() == 32 {
                state.overflowed = true;
            } else {
                state.rejected.push((promise, Persistent::save(&ctx, reason)));
            }
        })));
        Ok(Self { namespace: None, pending_promises: HashMap::new(), failures,
            context, runtime, termination, transport })
    }

    pub(super) fn handle(&self) -> ExecutionHandle {
        self.termination.clone()
    }

    pub(super) fn turn(&mut self, message: HostMessage) -> Result<Vec<RuntimeMessage>, RuntimeMessage> {
        let result = match message {
            HostMessage::Initialize { source } => self.load(&source).map(|()| vec![RuntimeMessage::Initialized]),
            HostMessage::Dispatch { id, export, return_mode } => self.dispatch(id, &export, return_mode),
            HostMessage::Close => Err(ErrorCode::InvalidProgram),
        };
        result.map_err(|code| RuntimeMessage::Error {
            code, script_error: self.failures.borrow_mut().script_error.take(),
        })
    }

    pub(super) fn load(&mut self, source: &str) -> Result<(), ErrorCode> {
        if self.namespace.is_some() || source.len() > MAX_SOURCE {
            return Err(ErrorCode::InvalidProgram);
        }
        self.context.with(|ctx| {
            let module = Module::declare(ctx.clone(), "plugin.mjs", source)
                .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::InvalidProgram))?;
            let (module, evaluation) = module.eval()
                .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::InvalidProgram))?;
            checkpoint(&ctx, &self.failures)?;
            evaluation.result::<Value>().ok_or(ErrorCode::ExecutionFailed)?
                .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::ExecutionFailed))?;
            let namespace = module.namespace()
                .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::InvalidProgram))?;
            self.namespace = Some(Persistent::save(&ctx, namespace));
            Ok(())
        })
    }

    pub(super) fn dispatch(&mut self, id: u64, export: &str, return_mode: ReturnMode)
        -> Result<Vec<RuntimeMessage>, ErrorCode> {
        let namespace = self.namespace.as_ref().ok_or(ErrorCode::InvalidProgram)?;
        if export.is_empty() || export.len() > MAX_METHOD || self.pending_promises.contains_key(&id) {
            return Err(ErrorCode::QuotaExceeded);
        }
        self.context.with(|ctx| {
            let namespace = namespace.clone().restore(&ctx)
                .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::InvalidProgram))?;
            let function: Function = namespace.get(export)
                .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::InvalidProgram))?;
            let result: Value = function.call(())
                .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::ExecutionFailed))?;
            let mut replies = Vec::new();
            if return_mode == ReturnMode::MaybeAsync && result.is_promise() {
                if self.pending_promises.len() >= MAX_PENDING_PROMISES {
                    return Err(ErrorCode::QuotaExceeded);
                }
                let promise = result.into_promise().ok_or(ErrorCode::InvalidResult)?;
                self.pending_promises.insert(id, Persistent::save(&ctx, promise));
            } else {
                replies.push(RuntimeMessage::Reply { id, value: result_string(result)? });
            }
            checkpoint(&ctx, &self.failures)?;
            let mut completed = Vec::new();
            for (&id, promise) in &self.pending_promises {
                let promise = promise.clone().restore(&ctx)
                    .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::ExecutionFailed))?;
                if promise.state() == PromiseState::Pending { continue; }
                let result = promise.result::<Value>().ok_or(ErrorCode::ExecutionFailed)?
                    .map_err(|error| capture(&ctx, &self.failures, error, ErrorCode::ExecutionFailed))?;
                replies.push(RuntimeMessage::Reply { id, value: result_string(result)? });
                completed.push(id);
            }
            for id in completed { self.pending_promises.remove(&id); }
            if self.pending_promises.contains_key(&id) {
                replies.push(RuntimeMessage::Pending { id });
            }
            Ok(replies)
        })
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.namespace.take();
        self.pending_promises.clear();
        self.failures.borrow_mut().rejected.clear();
        self.runtime.set_host_promise_rejection_tracker(None);
    }
}

fn checkpoint(ctx: &Ctx<'_>, failures: &Rc<RefCell<Failures>>) -> Result<(), ErrorCode> {
    loop {
        let mut job_context = std::ptr::null_mut();
        // SAFETY: each engine owns one context; this executor holds its runtime
        // lock throughout the turn, and no other thread enters the runtime.
        let result = unsafe { rquickjs::qjs::JS_ExecutePendingJob(
            rquickjs::qjs::JS_GetRuntime(ctx.as_raw().as_ptr()), &mut job_context) };
        if result < 0 {
            return Err(capture(ctx, failures, rquickjs::Error::Exception, ErrorCode::ExecutionFailed));
        }
        if result == 0 { break; }
    }
    let (rejected, overflowed) = {
        let mut state = failures.borrow_mut();
        (std::mem::take(&mut state.rejected), std::mem::take(&mut state.overflowed))
    };
    if let Some((_, reason)) = rejected.into_iter().next() {
        let reason = reason.restore(ctx).map_err(|_| ErrorCode::ExecutionFailed)?;
        record(ctx, failures, reason);
        return Err(ErrorCode::ExecutionFailed);
    }
    if overflowed { return Err(ErrorCode::QuotaExceeded); }
    Ok(())
}

fn result_string(value: Value<'_>) -> Result<String, ErrorCode> {
    let value = value.into_string().ok_or(ErrorCode::InvalidResult)?;
    let value = value.to_cstring().map_err(|_| ErrorCode::ExecutionFailed)?;
    if value.len() > MAX_RESULT { return Err(ErrorCode::QuotaExceeded); }
    Ok(value.as_str().to_owned())
}

fn capture(ctx: &Ctx<'_>, failures: &Rc<RefCell<Failures>>, error: rquickjs::Error, code: ErrorCode) -> ErrorCode {
    if error.is_exception() { record(ctx, failures, ctx.catch()); }
    code
}

fn record<'js>(ctx: &Ctx<'js>, failures: &Rc<RefCell<Failures>>, reason: Value<'js>) {
    let (line, column) = reason.as_object().map(|object| (
        object.get::<_, Option<usize>>("lineNumber").ok().flatten(),
        object.get::<_, Option<usize>>("columnNumber").ok().flatten(),
    )).unwrap_or_default();
    if let Ok(Coerced(mut message)) = Coerced::<String>::from_js(ctx, reason) {
        let truncated = message.len() > MAX_SCRIPT_ERROR_BYTES;
        if truncated { message.truncate(message.floor_char_boundary(MAX_SCRIPT_ERROR_BYTES)); }
        failures.borrow_mut().script_error = Some(PluginScriptError { message, line, column, truncated });
    }
}
