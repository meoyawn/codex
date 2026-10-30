mod grpc_session;
mod remote_session;

pub use codex_code_mode_protocol::*;
pub use grpc_session::GrpcCodeModeSessionProvider;
pub use remote_session::DisabledCodeModeSessionProvider;
pub use remote_session::ProcessOwnedCodeModeSession;
pub use remote_session::ProcessOwnedCodeModeSessionProvider;

/// Local sessions execute on supervised Rust threads inside the app-server.
#[derive(Default)]
pub struct InProcessCodeModeSessionProvider;

impl CodeModeSessionProvider for InProcessCodeModeSessionProvider {
    fn create_session(&self) -> CodeModeSessionProviderFuture<'_> {
        self.create_session_with_limits(CodeModeSessionCellExecutionLimits::default())
    }

    fn create_session_with_limits<'a>(
        &'a self,
        limits: CodeModeSessionCellExecutionLimits,
    ) -> CodeModeSessionProviderFuture<'a> {
        Box::pin(async move {
            Ok(std::sync::Arc::new(
                codex_code_mode_runtime::InProcessCodeModeSession::with_limits(limits),
            ) as std::sync::Arc<dyn CodeModeSession>)
        })
    }
}
