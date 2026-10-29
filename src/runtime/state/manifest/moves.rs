//! Intents for logical moves never confer physical deletion authority.
use super::RuntimeState;
use crate::common::{MidgeError, MidgeResult};
use crate::metadata::FileMeta;
use crate::runtime::{IntentLogEntry, PublicationPhase};

impl RuntimeState {
    pub(crate) fn record_sst_move_intent(
        &mut self,
        captured: Vec<FileMeta>,
        target_level: u32,
    ) -> MidgeResult<()> {
        crate::metadata::manifest::validate_sst_move(
            &self.manifest.files,
            &captured,
            target_level,
        )?;
        let mut intents = self.intent_log.clone();
        intents.push(IntentLogEntry::MoveSstsPublish {
            phase: PublicationPhase::OutputDurable,
            captured,
            target_level,
        });
        self.persist_intent_entries(&intents)?;
        self.intent_log = intents;
        Ok(())
    }

    pub(crate) fn update_sst_move_intent(
        &mut self,
        captured: &[FileMeta],
        target: u32,
        phase: Option<PublicationPhase>,
    ) -> MidgeResult<()> {
        let mut intents = self.intent_log.clone();
        let mut matched = false;
        intents.retain_mut(|intent| {
            let IntentLogEntry::MoveSstsPublish {
                phase: current,
                captured: old,
                target_level,
            } = intent
            else {
                return true;
            };
            if *target_level != target
                || old.len() != captured.len()
                || !old.iter().zip(captured).all(|(left, right)| {
                    left.same_identity(right)
                        && left.sst_seq == right.sst_seq
                        && left.sublevel == right.sublevel
                })
            {
                return true;
            }
            matched = true;
            if let Some(phase) = phase {
                *current = phase;
                true
            } else {
                false
            }
        });
        if !matched {
            return Err(MidgeError::Fenced(
                "logical move intent ownership changed".into(),
            ));
        }
        self.persist_intent_entries(&intents)?;
        self.intent_log = intents;
        Ok(())
    }
}
