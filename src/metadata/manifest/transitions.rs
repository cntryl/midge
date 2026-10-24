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
    Deleted(String),
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
                ManifestStep::Deleted(name) => {
                    self.manifest.retired_ssts.remove(&name);
                }
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

/// The two schema decisions a column family's lifecycle records.
#[derive(Clone, Copy)]
pub(crate) enum FamilyLifecycleEdit<'a> {
    Create {
        id: u32,
        name: &'a str,
        created_at: u64,
    },
    Drop {
        id: u32,
        drop_sequence: u64,
        dropped_sst_names: &'a [String],
    },
}

impl<'a> FamilyLifecycleEdit<'a> {
    /// The lifecycle decision `edit` records, if it records one.
    pub(crate) fn of(edit: &'a ManifestEdit) -> Option<Self> {
        match edit {
            ManifestEdit::CreateColumnFamily {
                id,
                name,
                created_at,
            } => Some(Self::Create {
                id: *id,
                name,
                created_at: *created_at,
            }),
            ManifestEdit::DropColumnFamilyAt {
                id,
                drop_sequence,
                dropped_sst_names,
            } => Some(Self::Drop {
                id: *id,
                drop_sequence: *drop_sequence,
                dropped_sst_names,
            }),
            _ => None,
        }
    }

    const fn id(self) -> u32 {
        match self {
            Self::Create { id, .. } | Self::Drop { id, .. } => id,
        }
    }

    /// Whether `manifest` already holds the state this decision produces: the
    /// family live under this identity, or dropped at this sequence. The exact
    /// original create also counts once a later drop left its tombstone, so
    /// replaying it cannot revive a dropped family.
    pub(crate) fn is_reflected_in(self, manifest: &Manifest) -> bool {
        let frontier = manifest.flushed_through.get(self.id());
        manifest.column_families.iter().any(|cf| {
            cf.id == self.id()
                && match self {
                    Self::Create {
                        name, created_at, ..
                    } => {
                        cf.name == name
                            && cf.created_at == created_at
                            && match frontier {
                                Ok(FamilyFrontier::Live { .. }) => cf.deleted_at.is_none(),
                                Ok(FamilyFrontier::Dropped) => cf.deleted_at.is_some(),
                                Err(_) => false,
                            }
                    }
                    Self::Drop { drop_sequence, .. } => {
                        cf.deleted_at.is_some()
                            && cf.drop_sequence == Some(drop_sequence)
                            && matches!(frontier, Ok(FamilyFrontier::Dropped))
                    }
                }
        })
    }
}

/// The DDL registry stores schema decisions, not recovery frontiers.
pub(crate) fn apply_column_family_lifecycle(
    families: &mut Vec<ColumnFamilyMeta>,
    edit: FamilyLifecycleEdit<'_>,
) -> MidgeResult<()> {
    let id = edit.id();
    let live_names = families
        .iter()
        .filter(|cf| cf.deleted_at.is_none())
        .map(|cf| (cf.id, cf.name.as_str()));
    let family = plan_family_lifecycle(edit, families.iter().find(|cf| cf.id == id), live_names)?;
    if let Some(existing) = families.iter_mut().find(|cf| cf.id == id) {
        *existing = family;
    } else {
        families.push(family);
    }
    Ok(())
}

/// The family a lifecycle decision produces from `existing`. A create must not
/// take a name that another family in `live_names` holds.
fn plan_family_lifecycle<'n>(
    edit: FamilyLifecycleEdit<'_>,
    existing: Option<&ColumnFamilyMeta>,
    mut live_names: impl Iterator<Item = (u32, &'n str)>,
) -> MidgeResult<ColumnFamilyMeta> {
    match edit {
        FamilyLifecycleEdit::Create {
            id,
            name,
            created_at,
        } => {
            if id == 0
                || name == "default"
                || live_names.any(|(other, live)| other != id && live == name)
            {
                return Err(invalid_transition("default or duplicate live family name"));
            }
            if let Some(existing) = existing {
                // A checkpoint can already contain the dropped identity when
                // replay reaches its original create edit. Only the exact
                // original identity is idempotent; a new family must use a
                // fresh id and creation timestamp.
                if existing.name == name && existing.created_at == created_at {
                    return Ok(existing.clone());
                }
                return Err(invalid_transition(format!(
                    "family id {id} cannot be reused"
                )));
            }
            Ok(ColumnFamilyMeta {
                id,
                name: name.to_owned(),
                created_at,
                deleted_at: None,
                drop_sequence: None,
                dropped_sst_names: Vec::new(),
                reclaimed: false,
            })
        }
        FamilyLifecycleEdit::Drop {
            id,
            drop_sequence,
            dropped_sst_names,
        } => {
            let existing =
                existing.ok_or_else(|| invalid_transition(format!("unknown family {id}")))?;
            if id == 0 {
                return Err(invalid_transition("default family cannot be dropped"));
            }
            if existing.deleted_at.is_some() {
                if existing.drop_sequence == Some(drop_sequence) {
                    return Ok(existing.clone());
                }
                return Err(invalid_transition(format!(
                    "conflicting drop for family {id}"
                )));
            }
            let mut family = existing.clone();
            family.deleted_at = Some(millis_since_epoch());
            family.drop_sequence = Some(drop_sequence);
            family.dropped_sst_names = dropped_sst_names.to_vec();
            family.reclaimed = false;
            Ok(family)
        }
    }
}

/// A flush frontier is authority only for the SST published with it: in the
/// same outer batch, after an output of that family bounded by the freeze,
/// keeping that output, and advancing the family once.
fn check_flush_pairing(edit: &ManifestEdit) -> MidgeResult<()> {
    fn flatten<'a>(edit: &'a ManifestEdit, leaves: &mut Vec<&'a ManifestEdit>) {
        match edit {
            ManifestEdit::Batch(children) => {
                for child in children {
                    flatten(child, leaves);
                }
            }
            _ => leaves.push(edit),
        }
    }
    let mut leaves = Vec::new();
    flatten(edit, &mut leaves);
    for (index, leaf) in leaves.iter().enumerate() {
        let ManifestEdit::AdvanceFlushFrontier {
            cf_id,
            freeze_sequence,
        } = leaf
        else {
            continue;
        };
        let additions = leaves[..index]
            .iter()
            .filter_map(|edit| match edit {
                ManifestEdit::AddSst(file) if file.cf_id == *cf_id => Some(file),
                _ => None,
            })
            .collect::<Vec<_>>();
        if !matches!(edit, ManifestEdit::Batch(_))
            || *freeze_sequence == u64::MAX
            || additions.is_empty()
            || additions.iter().any(|file| {
                file.largest_seq
                    .is_none_or(|sequence| sequence > *freeze_sequence)
            })
        {
            return Err(MidgeError::Corruption(
                "flush frontier must follow its SST in the same outer manifest batch".into(),
            ));
        }
        if leaves.iter().any(|edit| {
            matches!(edit, ManifestEdit::RemoveSst { name }
                if additions.iter().any(|file| file.name == *name))
        }) {
            return Err(MidgeError::Corruption(
                "flush batch removes its paired SST".into(),
            ));
        }
        if leaves[..index].iter().any(|edit| {
            matches!(edit, ManifestEdit::AdvanceFlushFrontier { cf_id: earlier, .. }
                if earlier == cf_id)
        }) {
            return Err(MidgeError::Corruption(
                "flush batch advances one family more than once".into(),
            ));
        }
    }
    Ok(())
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

    fn create_family(&mut self, id: u32, name: &str, created_at: u64) -> MidgeResult<()> {
        if self.family(id).is_some_and(|existing| {
            existing.deleted_at.is_some()
                && existing.name == name
                && existing.created_at == created_at
        }) {
            // Recovery can replay the original create edit after a
            // checkpoint installed its permanent tombstone. Recognize
            // it before checking whether a later family reused the name.
            return Ok(());
        }
        let live_names = self
            .families
            .values()
            .chain(
                self.base
                    .column_families
                    .iter()
                    .filter(|cf| !self.families.contains_key(&cf.id)),
            )
            .filter(|cf| cf.deleted_at.is_none())
            .map(|cf| (cf.id, cf.name.as_str()));
        let create = FamilyLifecycleEdit::Create {
            id,
            name,
            created_at,
        };
        let family = plan_family_lifecycle(create, self.family(id), live_names)?;
        let frontier = if self.family(id).is_some() {
            self.frontier(id)?
        } else {
            FamilyFrontier::Live {
                through_sequence: 0,
            }
        };
        if frontier == FamilyFrontier::Dropped {
            return Err(invalid_transition("dropped family cannot be revived"));
        }
        self.families.insert(id, family.clone());
        self.frontiers.insert(id, frontier);
        self.steps.push(ManifestStep::Family(family, frontier));
        Ok(())
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
        let normalized = FamilyLifecycleEdit::Drop {
            id,
            drop_sequence,
            dropped_sst_names: &names,
        };
        let family = plan_family_lifecycle(normalized, self.family(id), std::iter::empty())?;
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
        if self.base.retired_ssts.contains_key(&file.name)
            || self.files.get(&file.name).is_some_and(Option::is_none)
        {
            return Err(invalid_transition("cannot reuse a retired SST identity"));
        }
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

    /// Validate a whole outer edit: its flush pairing, then every transition.
    pub(super) fn validate(&mut self, edit: &ManifestEdit) -> MidgeResult<()> {
        check_flush_pairing(edit)?;
        self.apply(edit)
    }

    fn apply(&mut self, edit: &ManifestEdit) -> MidgeResult<()> {
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
            ManifestEdit::CompleteSstDelete { file } => {
                if self.file(&file.name).is_some() {
                    return Err(invalid_transition(
                        "cannot confirm deletion of an active SST",
                    ));
                }
                if let Some(original) = self.base.retired_ssts.get(&file.name) {
                    if !original.same_identity(file) {
                        return Err(invalid_transition(
                            "delete confirmation differs from retired identity",
                        ));
                    }
                    self.steps.push(ManifestStep::Deleted(file.name.clone()));
                }
            }
            ManifestEdit::CreateColumnFamily {
                id,
                name,
                created_at,
            } => self.create_family(*id, name, *created_at)?,
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
