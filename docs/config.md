# Configuration

For basic configuration instructions, see [this documentation](https://developers.openai.com/codex/config-basic).

For advanced configuration instructions, see [this documentation](https://developers.openai.com/codex/config-advanced).

For a full configuration reference, see [this documentation](https://developers.openai.com/codex/config-reference).

## Local MCP process ownership in this fork

Configured local stdio MCP servers are shared within one Codex backend process:

- A server declared in the global config has one instance across all projects and conversations in that backend.
- A server declared in a project's `.codex/config.toml` has one instance for that project.
- The same command declared in another project's config has a separate instance.

Ownership follows the config file and server name, not just the executable or
the conversation's working directory. A project that changes a server's launch
settings owns its own instance; an overlay that only changes tool policy keeps
the original server's owner. Profile-specific declarations are separate owners.
An explicit `cwd` takes precedence. Otherwise a global server starts in its
config directory and a project server starts in the project root. Shared servers
retain project roots, app connections and other service state across conversations;
target apps explicitly with identifiers such as Dart MCP's `appUri`.

The backend keeps each conversation's MCP client, approval policy and request
routing separate. MCP 2026 requests can run concurrently. Older protocol requests
are serialized so callbacks without a parent request ID reach the calling
conversation; legacy clients must negotiate compatible capabilities. The optional
`codex/auth-change` client offer is ignored in this comparison when the server
does not advertise support, so standalone discovery and chat clients can share
servers such as Dart MCP in either startup order. Servers that opt into the
extension still require matching capabilities. Closing one
conversation releases its connection; it does not kill
a server another conversation uses. The final disconnect starts a 60-second idle
timeout, after which the server process group is terminated. Changing transport
settings retires the old connections before replacing the shared process.

The registry uses in-memory transports and belongs to the backend's lifetime.
Separate Codex processes have separate registries. Plugin-owned servers, remote
executor transports, session-only overrides and externally hosted HTTP servers
retain their existing ownership.

## Lifecycle hooks

Admins can set top-level `allow_managed_hooks_only = true` in
`requirements.toml` to ignore user, project, and session hook configs while
still allowing managed hooks from requirements and managed config layers. This
setting is only supported in `requirements.toml`; putting it in `config.toml`
does not enable managed-hooks-only mode.
