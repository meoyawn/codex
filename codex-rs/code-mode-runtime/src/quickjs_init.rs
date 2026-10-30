/// Legacy host CLI configuration retained for source compatibility. QuickJS is
/// an interpreter and never generates executable code, in either mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum V8JitMode {
    #[default]
    Enabled,
    Disabled,
}

/// Compatibility entry point for upstream host consumers. No global engine or
/// JIT initialization is required by the embedded QuickJS runtime.
pub fn initialize_v8(_jit_mode: V8JitMode) -> Result<(), String> {
    Ok(())
}
