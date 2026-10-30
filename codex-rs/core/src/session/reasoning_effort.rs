//! Cache-preserving effort updates and the conversation's request-effort baseline.
//!
//! Only trusted harness items establish overrides. Replay preserves startup prewarm's
//! baseline while it is retained.
//! Compaction preserves the request baseline and reasserts the selected effort with a new item.
//! Fixed-effort workers always use their selected request-level effort.
//! Unsupported models use selected request effort without rewriting saved updates.

use super::session::Session;
use super::step_context::StepContext;
use super::step_settings::ResolvedStepSettings;
use super::step_settings::StepSettings;
use super::turn_context::TurnContext;
use crate::config::ConstraintError;
use crate::config::ConstraintResult;
use crate::state::ReasoningEffortPin;
use codex_history::CodexHarnessMetadata;
use codex_history::ResponseItemEnvelope;
use codex_protocol::models::ConfigurationReasoning;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::openai_models::ModelInfo;

/// Sampling can establish a pin; compaction must not change live state before it succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestEffortUsage {
    Sampling,
    Compaction,
}

impl Session {
    /// Reject effort changes unless this session can express them as history items.
    /// Use the startup catalog snapshot, not a later catalog refresh.
    pub(super) fn validate_cache_preserving_effort(
        &self,
        current: &StepSettings,
        next: &StepSettings,
    ) -> ConstraintResult<()> {
        if current.collaboration_mode.reasoning_effort() == next.collaboration_mode.reasoning_effort() {
            return Ok(());
        }
        let model = self.services.thread_extension_data.get::<ModelInfo>();
        if let Some(model) = model {
            let resolve = |settings: &StepSettings| {
                settings.collaboration_mode.reasoning_effort()
                    .or_else(|| model.default_reasoning_level.clone())
                    .map(|effort| model.resolve_reasoning_effort(effort))
            };
            let previous = resolve(current);
            let selected = resolve(next);
            if previous == selected {
                return Ok(());
            }
            if self.services.model_client.reasoning_effort_override_enabled(&model)
                && selected.is_some_and(|effort| !matches!(effort, ReasoningEffort::Custom(value) if value != "disabled"))
            {
                return Ok(());
            }
        }
        Err(ConstraintError::PromptCacheInvalidation { field_name: "reasoning effort" })
    }

    /// Establishes the selected effort in surviving history, independent of replayed settings.
    pub(crate) async fn record_reasoning_effort_override(&self, step_context: &StepContext) {
        self.record_reasoning_effort_override_for_settings(step_context.turn.as_ref(), &step_context.settings).await;
    }

    pub(super) async fn record_reasoning_effort_override_for_settings(
        &self,
        turn: &TurnContext,
        settings: &ResolvedStepSettings,
    ) {
        let Some(effort) = self.effort_for_configuration_update(settings) else {
            return;
        };
        let (should_skip, request_effort) = {
            let state = self.state.lock().await;
            let compacted = state.history.annotated_items().iter().any(|envelope| matches!(
                envelope.item, ResponseItem::Compaction { .. } | ResponseItem::ContextCompaction { .. }
            ));
            let latest_override = state
                .history
                .annotated_items()
                .iter()
                .rev()
                .take_while(|envelope| !matches!(
                    envelope.item, ResponseItem::Compaction { .. } | ResponseItem::ContextCompaction { .. }
                ))
                .enumerate()
                .find_map(|(index, envelope)| {
                    if !envelope
                        .metadata
                        .as_ref()
                        .is_some_and(|metadata| metadata.harness_authored_configuration)
                    {
                        return None;
                    }
                    match &envelope.item {
                        ResponseItem::ConfigurationUpdate { reasoning } => {
                            Some((index, &reasoning.effort))
                        }
                        _ => None,
                    }
                });
            // Recovery adds no user message. Reuse a matching trusted tail update even
            // before this runtime establishes its pin, rather than append adjacent updates.
            let skip = latest_override.is_some_and(|(index, established)| index == 0 && established == &effort)
                || state
                    .reasoning_effort_pin
                    .get(&settings.model_info.slug)
                    .is_some_and(|pinned| {
                        latest_override.map_or(!compacted && pinned == effort, |(_, established)| established == &effort)
                    });
            (skip, state.reasoning_effort_pin.get(&settings.model_info.slug).unwrap_or_else(|| effort.clone()))
        };
        if should_skip {
            return;
        }

        self.record_annotated_conversation_items(
            turn,
            &settings.model_info,
            vec![ResponseItemEnvelope {
                item: ResponseItem::ConfigurationUpdate {
                    reasoning: ConfigurationReasoning { effort },
                },
                metadata: Some(CodexHarnessMetadata {
                    harness_authored_configuration: true,
                    request_reasoning_effort: Some(request_effort),
                    ..Default::default()
                }),
            }],
        )
        .await;
    }

    /// Sampling and compaction share the conversation's original request effort.
    pub(crate) async fn reasoning_effort_for_request(
        &self,
        settings: &ResolvedStepSettings,
        usage: RequestEffortUsage,
    ) -> Option<ReasoningEffort> {
        let selected_effort = settings.reasoning_effort().cloned();
        if !self
            .services
            .model_client
            .reasoning_effort_override_enabled(&settings.model_info)
        {
            if usage == RequestEffortUsage::Sampling {
                self.state.lock().await.reasoning_effort_pin = ReasoningEffortPin::Unset;
            }
            return selected_effort;
        }
        if usage == RequestEffortUsage::Compaction
            && let Some(pinned) = self
                .state
                .lock()
                .await
                .reasoning_effort_pin
                .get(&settings.model_info.slug)
        {
            return Some(pinned);
        }
        let effort = self.effort_for_configuration_update(settings);
        let mut state = self.state.lock().await;
        let Some(effort) = effort else {
            if usage == RequestEffortUsage::Sampling {
                state.reasoning_effort_pin = ReasoningEffortPin::Unset;
            }
            return selected_effort;
        };
        Some(match usage {
            RequestEffortUsage::Sampling => state
                .reasoning_effort_pin
                .pin(&settings.model_info.slug, effort),
            // Failed compaction and fallback-model lookups must not mutate the live pin.
            RequestEffortUsage::Compaction => effort,
        })
    }

    fn effort_for_configuration_update(
        &self,
        settings: &ResolvedStepSettings,
    ) -> Option<ReasoningEffort> {
        if !self
            .services
            .model_client
            .reasoning_effort_override_enabled(&settings.model_info)
        {
            return None;
        }
        let effort = settings
            .model_info
            .resolve_reasoning_effort(settings.effective_reasoning_effort()?);
        // Persistent normalizes to "disabled". Keep unknown custom values out of
        // durable updates so injected items stay bounded to known backend modes.
        if matches!(&effort, ReasoningEffort::Custom(value) if value != "disabled") {
            return None;
        }
        Some(effort)
    }
}

#[cfg(test)]
#[path = "reasoning_effort_tests.rs"]
mod tests;
