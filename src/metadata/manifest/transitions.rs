use super::{
    millis_since_epoch, ColumnFamilyMeta, FamilyFrontier, FileMeta, HashMap, Manifest,
    ManifestEdit, MidgeError, MidgeResult,
};

/// A transition validated against exclusively borrowed manifest state.
/// Dropping it leaves the manifest unchanged; commit has no fallible rules.
pub struct PreparedManifestEdit<'a> {
    pub(super) manifest: &'a mut Manifest,
    pub(super) steps: Vec<ManifestStep>,
}

pub(super) enum ManifestStep {
    Add(FileMeta),
    Remove(String),
    Family(ColumnFamilyMeta, FamilyFrontier),
    Frontier(u32, FamilyFrontier),
    Next(u32, u64),
}

impl PreparedManifestEdit<'_> {
    pub fn commit(self) {
        for step in self.steps {
            match step {
                ManifestStep::Add(file) => self.manifest.add_file(file),
                ManifestStep::Remove(name) => self.manifest.remove_file(&name),
                ManifestStep::Family(family, frontier) => {
                    self.manifest
                        .flushed_through
                        .set_validated(family.id, frontier);
                    if let Some(existing) = self
                        .manifest
                        .column_families
                        .iter_mut()
                        .find(|cf| cf.id == family.id)
                    {
                        *existing = family;
                    } else {
                        self.manifest.column_families.push(family);
                    }
                }
                ManifestStep::Frontier(id, value) => {
                    self.manifest.flushed_through.set_validated(id, value);
                }
                ManifestStep::Next(id, next) => {
                    self.manifest.next_sst_seqs.insert(id, next);
                }
            }
        }
    }
}

pub(super) fn invalid_transition(message: impl Into<String>) -> MidgeError {
    MidgeError::Corruption(format!("invalid manifest transition: {}", message.into()))
}

/// The DDL registry stores schema decisions, not recovery frontiers.
pub(crate) fn apply_column_family_lifecycle(
    families: &mut Vec<ColumnFamilyMeta>,
    edit: &ManifestEdit,
) -> MidgeResult<()> {
    let id = match edit {
        ManifestEdit::CreateColumnFamily { id, .. }
        | ManifestEdit::DropColumnFamilyAt { id, .. } => *id,
        _ => {
            return Err(invalid_transition(
                "schema registry accepts only create/drop edits",
            ))
        }
    };
    let existing = families.iter().find(|cf| cf.id == id);
    let collision = matches!(edit, ManifestEdit::CreateColumnFamily { name, .. }
        if families.iter().any(|cf| cf.id != id && cf.name == *name && cf.deleted_at.is_none()));
    let family = plan_family_lifecycle(edit, existing, collision)?;
    if let Some(existing) = families.iter_mut().find(|cf| cf.id == id) {
        *existing = family;
    } else {
        families.push(family);
    }
    Ok(())
}

fn plan_family_lifecycle(
    edit: &ManifestEdit,
    existing: Option<&ColumnFamilyMeta>,
    collision: bool,
) -> MidgeResult<ColumnFamilyMeta> {
    match edit {
        ManifestEdit::CreateColumnFamily {
            id,
            name,
            created_at,
        } => {
            if *id == 0 || name == "default" || collision {
                return Err(invalid_transition("default or duplicate live family name"));
            }
            if let Some(existing) = existing {
                if existing.deleted_at.is_none()
                    && existing.name == *name
                    && existing.created_at == *created_at
                {
                    return Ok(existing.clone());
                }
                return Err(invalid_transition(format!(
                    "family id {id} cannot be reused"
                )));
            }
            Ok(ColumnFamilyMeta {
                id: *id,
                name: name.clone(),
                created_at: *created_at,
                deleted_at: None,
                drop_sequence: None,
                dropped_sst_names: Vec::new(),
                reclaimed: false,
            })
        }
        ManifestEdit::DropColumnFamilyAt {
            id,
            drop_sequence,
            dropped_sst_names,
        } => {
            let existing =
                existing.ok_or_else(|| invalid_transition(format!("unknown family {id}")))?;
            if *id == 0 {
                return Err(invalid_transition("default family cannot be dropped"));
            }
            if existing.deleted_at.is_some() {
                if existing.drop_sequence == Some(*drop_sequence) {
                    return Ok(existing.clone());
                }
                return Err(invalid_transition(format!(
                    "conflicting drop for family {id}"
                )));
            }
            let mut family = existing.clone();
            family.deleted_at = Some(millis_since_epoch());
            family.drop_sequence = Some(*drop_sequence);
            family.dropped_sst_names.clone_from(dropped_sst_names);
            family.reclaimed = false;
            Ok(family)
        }
        _ => Err(invalid_transition("expected family lifecycle edit")),
    }
}

pub(super) struct EditValidator<'a> {
    base: &'a Manifest,
    families: HashMap<u32, ColumnFamilyMeta>,
    frontiers: HashMap<u32, FamilyFrontier>,
    files: HashMap<String, Option<FileMeta>>,
    counters: HashMap<u32, u64>,
    pub(super) steps: Vec<ManifestStep>,
}

impl<'a> EditValidator<'a> {
    pub(super) fn new(base: &'a Manifest) -> Self {
        Self {
            base,
            families: HashMap::new(),
            frontiers: HashMap::new(),
            files: HashMap::new(),
            counters: HashMap::new(),
            steps: Vec::new(),
        }
    }

    fn frontier(&self, id: u32) -> MidgeResult<FamilyFrontier> {
        self.frontiers
            .get(&id)
            .copied()
            .map_or_else(|| self.base.flushed_through.get(id), Ok)
    }

    fn family(&self, id: u32) -> Option<&ColumnFamilyMeta> {
        self.families
            .get(&id)
            .or_else(|| self.base.column_families.iter().find(|cf| cf.id == id))
    }

    fn file(&self, name: &str) -> Option<&FileMeta> {
        self.files.get(name).map_or_else(
            || self.base.files.iter().find(|file| file.name == name),
            Option::as_ref,
        )
    }

    fn drop_family(
        &mut self,
        id: u32,
        drop_sequence: u64,
        dropped_sst_names: &[String],
    ) -> MidgeResult<()> {
        self.frontier(id)?;
        let mut names = dropped_sst_names.to_vec();
        if names.is_empty() {
            names.extend(
                self.base
                    .files
                    .iter()
                    .filter(|file| file.cf_id == id && self.file(&file.name).is_some())
                    .map(|file| file.name.clone()),
            );
            names.extend(
                self.files
                    .values()
                    .filter_map(Option::as_ref)
                    .filter(|file| file.cf_id == id)
                    .map(|file| file.name.clone()),
            );
            names.sort();
            names.dedup();
        }
        for name in &names {
            if self.file(name).is_some_and(|file| file.cf_id != id) {
                return Err(invalid_transition("drop references another family's SST"));
            }
        }
        let normalized = ManifestEdit::DropColumnFamilyAt {
            id,
            drop_sequence,
            dropped_sst_names: names,
        };
        let family = plan_family_lifecycle(&normalized, self.family(id), false)?;
        self.families.insert(id, family.clone());
        self.frontiers.insert(id, FamilyFrontier::Dropped);
        self.steps
            .push(ManifestStep::Family(family, FamilyFrontier::Dropped));
        Ok(())
    }

    fn reclaim_family(&mut self, id: u32, names: &[String]) -> MidgeResult<()> {
        if self.frontier(id)? != FamilyFrontier::Dropped {
            return Err(invalid_transition("cannot reclaim live family"));
        }
        let mut family = self
            .family(id)
            .ok_or_else(|| invalid_transition("unknown reclaim family"))?
            .clone();
        for name in names {
            if self.file(name).is_some_and(|file| file.cf_id != id) {
                return Err(invalid_transition(
                    "reclaim references another family's SST",
                ));
            }
            family
                .dropped_sst_names
                .retain(|candidate| candidate != name);
            self.files.insert(name.clone(), None);
            self.steps.push(ManifestStep::Remove(name.clone()));
        }
        family.reclaimed = family.dropped_sst_names.is_empty();
        self.families.insert(id, family.clone());
        self.steps
            .push(ManifestStep::Family(family, FamilyFrontier::Dropped));
        Ok(())
    }

    fn add_sst(&mut self, file: &FileMeta) -> MidgeResult<()> {
        if self.frontier(file.cf_id)? == FamilyFrontier::Dropped {
            return Err(invalid_transition("cannot add SST to dropped family"));
        }
        if self
            .file(&file.name)
            .is_some_and(|old| !old.same_identity(file))
        {
            return Err(invalid_transition(format!(
                "conflicting SST identity {}",
                file.name
            )));
        }
        self.files.insert(file.name.clone(), Some(file.clone()));
        self.steps.push(ManifestStep::Add(file.clone()));
        Ok(())
    }

    pub(super) fn apply(&mut self, edit: &ManifestEdit) -> MidgeResult<()> {
        match edit {
            ManifestEdit::Batch(edits) => {
                for child in edits {
                    self.apply(child)?;
                }
            }
            ManifestEdit::AddSst(file) => self.add_sst(file)?,
            ManifestEdit::RemoveSst { name } => {
                self.files.insert(name.clone(), None);
                self.steps.push(ManifestStep::Remove(name.clone()));
            }
            ManifestEdit::CreateColumnFamily { id, name, .. } => {
                let collision = self
                    .families
                    .values()
                    .chain(
                        self.base
                            .column_families
                            .iter()
                            .filter(|cf| !self.families.contains_key(&cf.id)),
                    )
                    .any(|cf| cf.id != *id && cf.name == *name && cf.deleted_at.is_none());
                let family = plan_family_lifecycle(edit, self.family(*id), collision)?;
                let frontier = if self.family(*id).is_some() {
                    self.frontier(*id)?
                } else {
                    FamilyFrontier::Live {
                        through_sequence: 0,
                    }
                };
                if frontier == FamilyFrontier::Dropped {
                    return Err(invalid_transition("dropped family cannot be revived"));
                }
                self.families.insert(*id, family.clone());
                self.frontiers.insert(*id, frontier);
                self.steps.push(ManifestStep::Family(family, frontier));
            }
            ManifestEdit::DropColumnFamilyAt {
                id,
                drop_sequence,
                dropped_sst_names,
            } => self.drop_family(*id, *drop_sequence, dropped_sst_names)?,
            ManifestEdit::ReclaimColumnFamily { id, names } => self.reclaim_family(*id, names)?,
            ManifestEdit::AdvanceFlushFrontier {
                cf_id,
                freeze_sequence,
            } => {
                let FamilyFrontier::Live {
                    through_sequence: current,
                } = self.frontier(*cf_id)?
                else {
                    return Err(invalid_transition("cannot advance dropped family"));
                };
                if *freeze_sequence == u64::MAX || *freeze_sequence < current {
                    return Err(invalid_transition("reserved or regressing flush frontier"));
                }
                let publishes_new_output = self.steps.iter().any(|step| {
                    matches!(step, ManifestStep::Add(file)
                        if file.cf_id == *cf_id
                            && !self.base.files.iter().any(|existing| existing.name == file.name))
                });
                if *freeze_sequence > current && !publishes_new_output {
                    return Err(invalid_transition(
                        "existing SST cannot establish a higher flush frontier",
                    ));
                }
                let value = FamilyFrontier::Live {
                    through_sequence: *freeze_sequence,
                };
                self.frontiers.insert(*cf_id, value);
                self.steps.push(ManifestStep::Frontier(*cf_id, value));
            }
            ManifestEdit::BumpNextSstSeq { cf_id, next_seq } => {
                self.frontier(*cf_id)?;
                let current = self
                    .counters
                    .get(cf_id)
                    .copied()
                    .or_else(|| self.base.next_sst_seqs.get(cf_id).copied())
                    .unwrap_or(0);
                let next = current.max(*next_seq);
                self.counters.insert(*cf_id, next);
                self.steps.push(ManifestStep::Next(*cf_id, next));
            }
        }
        Ok(())
    }
}
