//! Lightweight UI isolates share one bounded executor in the plugin process.
//! No ordinary CodeMode tools or globals.
//! Script failures discard their isolate without affecting its peers.

#[path = "quickjs_engine.rs"]
mod engine;
mod transport;
mod watchdog;
mod workers;

use std::io;

pub fn run() -> io::Result<()> {
    workers::run()
}
