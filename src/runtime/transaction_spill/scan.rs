use super::format::{previous_sparse_offset, read_op_primary_key_frame, RunFile, RunHeader};
use super::{op_primary_key_bytes, SpillRun, TransactionWriteSet, RUN_HEADER_LEN};
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;
use std::sync::Arc;

enum ReverseChunkSource {
    Materialized {
        chunks: Vec<(u64, u64)>,
        next: usize,
        pool: Arc<super::TransactionMemoryPool>,
        charge: usize,
    },
    Streaming {
        header: RunHeader,
        next_end: u64,
    },
}

enum RunKeyDirection {
    Forward,
    Reverse {
        chunks: ReverseChunkSource,
        keys: std::vec::IntoIter<Bytes>,
    },
}

pub(super) struct RunKeyCursor {
    path: std::path::PathBuf,
    cursor: u64,
    data_end: u64,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    direction: RunKeyDirection,
    previous_key: Option<Bytes>,
    exhausted: bool,
}

impl RunKeyCursor {
    pub(super) fn new(
        run: &SpillRun,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        reverse: bool,
    ) -> MidgeResult<Self> {
        // The reader cache supplies the header and any admitted sparse index.
        // Reverse scans separately reserve their chunk table, then stream the
        // sparse frames when the transaction pool refuses that reservation.
        let header = run.header()?;
        let (cursor, direction) = if reverse {
            let charge = header
                .sparse_count
                .checked_mul(size_of::<(u64, u64)>())
                .ok_or_else(|| {
                    MidgeError::Corruption(
                        "transaction spill sparse chunk reservation overflow".to_string(),
                    )
                })?;
            let chunks = if run.pool.try_reserve(charge) {
                match run.sparse_chunks() {
                    Ok(chunks) => ReverseChunkSource::Materialized {
                        next: chunks.len(),
                        chunks,
                        pool: Arc::clone(&run.pool),
                        charge,
                    },
                    Err(error) => {
                        run.pool.release(charge);
                        return Err(error);
                    }
                }
            } else {
                ReverseChunkSource::Streaming {
                    header,
                    next_end: header.ordinal_table_offset,
                }
            };
            (
                RUN_HEADER_LEN as u64,
                RunKeyDirection::Reverse {
                    chunks,
                    keys: Vec::new().into_iter(),
                },
            )
        } else {
            (run.sparse_start(start)?, RunKeyDirection::Forward)
        };

        Ok(Self {
            path: run.path.clone(),
            cursor,
            data_end: header.ordinal_table_offset,
            start: start.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            direction,
            previous_key: None,
            exhausted: header.record_count == 0,
        })
    }

    fn key_in_bounds(&self, key: &[u8]) -> bool {
        self.start.as_deref().is_none_or(|start| key >= start)
            && self.end.as_deref().is_none_or(|end| key < end)
    }

    fn next_forward_key(&mut self) -> MidgeResult<Option<Bytes>> {
        let mut file = RunFile::open(&self.path)?;
        while self.cursor < self.data_end {
            file.seek_to(self.cursor)?;
            let (key, next_cursor) = read_op_primary_key_frame(&mut file)?;
            if next_cursor > self.data_end || next_cursor <= self.cursor {
                return Err(MidgeError::Corruption(
                    "transaction spill data frame exceeds its data section".to_string(),
                ));
            }
            self.cursor = next_cursor;
            if self.end.as_deref().is_some_and(|end| key.as_ref() >= end) {
                self.exhausted = true;
                return Ok(None);
            }
            if !self.key_in_bounds(&key)
                || self
                    .previous_key
                    .as_ref()
                    .is_some_and(|previous| previous == &key)
            {
                continue;
            }
            self.previous_key = Some(key.clone());
            return Ok(Some(key));
        }
        self.exhausted = true;
        Ok(None)
    }

    fn load_reverse_chunk(&mut self) -> MidgeResult<bool> {
        let mut file = RunFile::open(&self.path)?;
        let (chunk_start, chunk_end) = {
            let RunKeyDirection::Reverse { chunks, .. } = &mut self.direction else {
                return Ok(false);
            };
            match chunks {
                ReverseChunkSource::Materialized { chunks, next, .. } => {
                    if *next == 0 {
                        self.exhausted = true;
                        return Ok(false);
                    }
                    *next -= 1;
                    chunks[*next]
                }
                ReverseChunkSource::Streaming { header, next_end } => {
                    let Some(chunk_start) = previous_sparse_offset(&mut file, header, *next_end)?
                    else {
                        self.exhausted = true;
                        return Ok(false);
                    };
                    let chunk_end = *next_end;
                    *next_end = chunk_start;
                    (chunk_start, chunk_end)
                }
            }
        };
        let mut cursor = chunk_start;
        let mut chunk_keys: Vec<Bytes> = Vec::new();
        while cursor < chunk_end {
            file.seek_to(cursor)?;
            let (key, next_cursor) = read_op_primary_key_frame(&mut file)?;
            if next_cursor > chunk_end || next_cursor <= cursor {
                return Err(MidgeError::Corruption(
                    "transaction spill sparse chunk does not align to operation frames".to_string(),
                ));
            }
            cursor = next_cursor;
            if self.key_in_bounds(&key) {
                chunk_keys.push(key);
            }
        }
        chunk_keys.dedup();
        chunk_keys.reverse();
        if let RunKeyDirection::Reverse { keys, .. } = &mut self.direction {
            *keys = chunk_keys.into_iter();
        }
        Ok(true)
    }

    fn next_reverse_key(&mut self) -> MidgeResult<Option<Bytes>> {
        loop {
            let key = match &mut self.direction {
                RunKeyDirection::Reverse { keys, .. } => keys.next(),
                RunKeyDirection::Forward => None,
            };
            if let Some(key) = key {
                if self
                    .previous_key
                    .as_ref()
                    .is_some_and(|previous| previous == &key)
                {
                    continue;
                }
                self.previous_key = Some(key.clone());
                return Ok(Some(key));
            }
            if !self.load_reverse_chunk()? {
                return Ok(None);
            }
        }
    }
}

impl Drop for RunKeyCursor {
    fn drop(&mut self) {
        if let RunKeyDirection::Reverse {
            chunks: ReverseChunkSource::Materialized { pool, charge, .. },
            ..
        } = &mut self.direction
        {
            pool.release(*charge);
            *charge = 0;
        }
    }
}

impl std::iter::Iterator for RunKeyCursor {
    type Item = MidgeResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let result = match self.direction {
            RunKeyDirection::Forward => self.next_forward_key(),
            RunKeyDirection::Reverse { .. } => self.next_reverse_key(),
        };
        match result {
            Ok(Some(key)) => Some(Ok(key)),
            Ok(None) => None,
            Err(error) => {
                self.exhausted = true;
                Some(Err(error))
            }
        }
    }
}

enum IntentKeyIterator {
    Resident(std::vec::IntoIter<Bytes>),
    Run(RunKeyCursor),
}

impl IntentKeyIterator {
    fn next(&mut self) -> Option<MidgeResult<Bytes>> {
        match self {
            Self::Resident(keys) => keys.next().map(Ok),
            Self::Run(keys) => keys.next(),
        }
    }
}

struct IntentKeySource {
    iterator: IntentKeyIterator,
    head: Option<MidgeResult<Bytes>>,
    primed: bool,
    needs_advance: bool,
}

impl IntentKeySource {
    fn new(iterator: IntentKeyIterator) -> Self {
        Self {
            iterator,
            head: None,
            primed: false,
            needs_advance: false,
        }
    }

    fn prime(&mut self) {
        if !self.primed {
            self.head = self.iterator.next();
            self.primed = true;
        }
    }

    fn advance_if_needed(&mut self) {
        if self.needs_advance {
            self.head = self.iterator.next();
            self.needs_advance = false;
        }
    }

    fn consume(&mut self) {
        self.head = None;
        self.needs_advance = true;
    }
}

/// K-way unique-key merge over resident intents and private spill runs.
pub(crate) struct IntentKeyScan {
    reverse: bool,
    sources: Vec<IntentKeySource>,
    exhausted: bool,
}

impl IntentKeyScan {
    pub(super) fn new(
        write_set: &TransactionWriteSet,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        reverse: bool,
    ) -> MidgeResult<Self> {
        let mut resident = write_set
            .resident
            .iter()
            .map(|entry| op_primary_key_bytes(&entry.op))
            .filter(|key| {
                start.is_none_or(|start| key.as_ref() >= start)
                    && end.is_none_or(|end| key.as_ref() < end)
            })
            .collect::<Vec<_>>();
        resident.sort_unstable();
        resident.dedup();
        if reverse {
            resident.reverse();
        }

        let mut sources = Vec::with_capacity(write_set.runs.len() + 1);
        sources.push(IntentKeySource::new(IntentKeyIterator::Resident(
            resident.into_iter(),
        )));
        for run in &write_set.runs {
            sources.push(IntentKeySource::new(IntentKeyIterator::Run(
                RunKeyCursor::new(run, start, end, reverse)?,
            )));
        }
        Ok(Self {
            reverse,
            sources,
            exhausted: false,
        })
    }

    fn next_key(&mut self) -> MidgeResult<Option<Bytes>> {
        for source in &mut self.sources {
            source.prime();
            source.advance_if_needed();
        }
        if let Some(error) = self.sources.iter_mut().find_map(|source| {
            if source.head.as_ref().is_some_and(Result::is_err) {
                source.head.take().and_then(Result::err)
            } else {
                None
            }
        }) {
            return Err(error);
        }

        let Some(selected) = self
            .sources
            .iter()
            .filter_map(|source| source.head.as_ref()?.as_ref().ok())
            .cloned()
            .reduce(|selected, candidate| {
                let candidate_wins = if self.reverse {
                    candidate > selected
                } else {
                    candidate < selected
                };
                if candidate_wins {
                    candidate
                } else {
                    selected
                }
            })
        else {
            return Ok(None);
        };

        for source in &mut self.sources {
            if source
                .head
                .as_ref()
                .and_then(|head| head.as_ref().ok())
                .is_some_and(|key| key == &selected)
            {
                source.consume();
            }
        }
        Ok(Some(selected))
    }
}

impl std::iter::Iterator for IntentKeyScan {
    type Item = MidgeResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.next_key() {
            Ok(Some(key)) => Some(Ok(key)),
            Ok(None) => {
                self.exhausted = true;
                None
            }
            Err(error) => {
                self.exhausted = true;
                Some(Err(error))
            }
        }
    }
}
