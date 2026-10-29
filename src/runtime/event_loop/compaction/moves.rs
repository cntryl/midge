use super::{CompactionCoordinator, EventLoop, PreparedCompactionOutput};
use crate::common::{MidgeError, MidgeResult};

impl CompactionCoordinator {
    pub(super) fn record_start_intent(
        event_loop: &mut EventLoop,
        token: &super::CompactionPublicationToken,
        added: &[crate::runtime::FileMeta],
    ) -> MidgeResult<(Vec<crate::metadata::FileMeta>, Vec<String>)> {
        if event_loop.actors.compaction_actor.active_trivial_move() {
            let captured = event_loop
                .actors
                .compaction_actor
                .captured_input_metadata()
                .to_vec();
            event_loop
                .state
                .record_sst_move_intent(captured.clone(), token.target_level)?;
            Ok((captured, Vec::new()))
        } else {
            let superseded = event_loop.state.record_compaction_publication_intent(
                token.cf_id,
                token.input_ssts.clone(),
                added.to_vec(),
            )?;
            Ok((Vec::new(), superseded))
        }
    }

    pub(super) fn validate_move_publication(
        event_loop: &EventLoop,
        inputs: &[String],
        outputs: &[String],
        cf_id: u32,
        target: u32,
    ) -> MidgeResult<(
        u64,
        Vec<PreparedCompactionOutput>,
        Vec<crate::runtime::FileMeta>,
    )> {
        let actor = &event_loop.actors.compaction_actor;
        let captured = actor.captured_input_metadata();
        let mut expected: Vec<_> = captured.iter().map(|file| file.name.clone()).collect();
        expected.sort_unstable();
        let mut canonical_inputs = inputs.to_vec();
        canonical_inputs.sort_unstable();
        if expected != canonical_inputs
            || expected != outputs
            || captured.iter().any(|file| file.cf_id != cf_id)
        {
            return Err(MidgeError::Fenced(
                "logical move completion changed its accepted sources".into(),
            ));
        }
        if crate::metadata::manifest::validate_sst_move(
            &event_loop.state.manifest.files,
            captured,
            target,
        )? {
            return Err(MidgeError::Fenced(
                "logical move source was already promoted before publication".into(),
            ));
        }
        let added = captured
            .iter()
            .map(|file| {
                let mut file: crate::runtime::FileMeta = file.into();
                file.level = target;
                file
            })
            .collect();
        Ok((
            actor.accepted_output_generation(cf_id, target)?,
            Vec::new(),
            added,
        ))
    }
}
