//! Protocol framing and child lifetime for locally spawned MCP servers.
//!
//! Process creation is platform-specific; protocol selection and shutdown are shared.

use std::future::Future;
use std::io;
use std::time::Duration;

use codex_utils_pty::Command;
use rmcp::service::RoleClient;
use rmcp::service::RxJsonRpcMessage;
use rmcp::service::TxJsonRpcMessage;
use rmcp::transport::Transport;
use tokio::process::ChildStderr;

use crate::bounded_stdio_transport::BoundedStdioTransport;
use crate::protocol_mode::McpProtocolMode;
use codex_utils_pty::Child;

pub(super) struct LocalStdioTransport {
    child: Box<Child>,
    transport: BoundedStdioTransport,
}

impl LocalStdioTransport {
    pub(super) fn spawn(
        command: Command,
        program_name: String,
        protocol_mode: McpProtocolMode,
    ) -> io::Result<(Self, Option<ChildStderr>)> {
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("MCP server stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("MCP server stdout was not piped"))?;
        let stderr = child.stderr.take();
        // Dropping an oversized response without closing the connection leaves
        // its request pending and blocks the entire shared legacy request queue.
        let transport = BoundedStdioTransport::new(stdin, stdout, program_name, protocol_mode);
        Ok((
            Self {
                child: Box::new(child),
                transport,
            },
            stderr,
        ))
    }

    pub(super) fn id(&self) -> Option<u32> {
        self.child.id()
    }
}

impl Transport<RoleClient> for LocalStdioTransport {
    type Error = io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        self.transport.send(item)
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        self.transport.receive()
    }

    async fn close(&mut self) -> io::Result<()> {
        self.transport.close().await?;
        match tokio::time::timeout(Duration::from_secs(3), self.child.wait()).await {
            Ok(status) => status.map(|_| ()),
            Err(_) => self.child.kill().await,
        }
    }
}
