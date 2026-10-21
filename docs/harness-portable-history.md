# Portable history for cross-host handoff

This fork adds the local `dev.harness.portableHistory` extension to app-server.
It completes the paginated-history transfer path used by the Flutter client.
The desktop Git snapshot/worktree transfer remains owned by the client. This is
an additive local RPC contract, not an OpenAI HTTP API or a replacement agent loop.

Initialize with `experimentalApi: true` and an extension offer containing
`version: 1` and the exact `PORTABLE_HISTORY_ROLLOUT_SCHEMA`. Compatible local
stores return `portableHistory` with the schema, byte limits and
`supportsAgentGraph: false`. Unnegotiated connections cannot call the methods.

- `harness/thread/portableHistory/export` freezes a durable paginated root at a
  terminal boundary under the existing fork lifecycle reservation. It flattens
  every inherited rollout prefix at its byte/ordinal cutoff, preserving raw
  typed replay, compactions and record timestamps. It returns a leased private
  file path, byte length, SHA-256, record count and terminal turn ID.
- The client streams those exact bytes between the captured hosts. An export is
  released with `harness/thread/portableHistory/release`; disconnect also drops
  the connection's leases. Export capacity and artifact sizes are bounded.
- `harness/thread/portableHistory/import` validates a private snapshot of the
  input before entering the existing fork/configuration path. The nested fork
  requires the source ID, an absolute destination cwd and `excludeTurns: true`.
  It creates a fresh durable root, copies the replay with original timestamps,
  assigns destination ordinals, and persists destination-effective settings.
  Foreign source IDs are provenance and never select local runtime callbacks,
  attachment memberships or instruction providers.

The destination has no foreign `history_base` dependency. Compaction limits the
model-visible context independently of the complete visible saved history; the
export does not substitute a UI projection or only the latest model-context
suffix. Cold reads and subsequent return handoffs use ordinary local storage.

No turn or goal continuation starts on import. V1 rejects source conversations
with stored subagent descendants, including archived descendants, and rejects
nonterminal or unsupported replay. Live workers and goals do not migrate.
Failures after fork startup are reported as an unknown creation outcome so the
client retains the worktree/artifact and cannot automatically create duplicates.

The thread-store unit tests exercise inherited cutoffs, compaction preservation,
timestamp-preserving destination persistence, a return export, digest/unknown
field rejection, and refusal of active history. The Flutter development tooling
also offers isolated stdio initialize and forward/import/cold-read/reverse-import
checks without model requests or access to the user's conversations.

The separate `dev.harness.buildIdentity` v1 initialize offer returns
`harnessBuild` only when the packager supplied `STABLE_GIT_COMMIT`,
`HARNESS_SOURCE_FINGERPRINT_SHA256` and `HARNESS_CARGO_PROFILE` at compilation.
The receipt identifies source bytes, commit, package version and Cargo profile,
independently of the host platform. Deployment uses it to verify the selected
shared daemon after upstream `daemon update --from-cli --yes`. Ordinary remote
connections reuse the host's selected daemon through `app-server proxy`; they
do not select a server based on a client's bundled binary. Build provenance is
separate from portable-history schema compatibility.
