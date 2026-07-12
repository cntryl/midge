use super::format::{
    read_header, read_op_primary_key_frame, read_sparse_offsets, sparse_start_for_key,
};
use super::{op_primary_key_bytes, SpillRun, TransactionWriteSet, RUN_HEADER_LEN};
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;
use std::fs::File;
use std::io::{Seek, SeekFrom};

enum RunKeyDirection {
    Forward,
    Reverse {
        chunks: Vec<(u64, u64)>,
        next_chunk: usize,
        keys: std::vec::IntoIter<Bytes>,
    },
}

pub(super) struct RunKeyCursor {
    file: File,
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
        let mut file = File::open(&run.path)?;
        let header = read_header(&mut file)?;
        if header.record_count != run.record_count {
            return Err(MidgeError::Corruption(
                "transaction spill record count changed".to_string(),
            ));
        }

        let (cursor, direction) = if reverse {
            let starts = read_sparse_offsets(&mut file, &header)?;
            let chunks = starts
                .iter()
                .enumerate()
                .map(|(index, chunk_start)| {
                    let chunk_end = starts
                        .get(index + 1)
                        .copied()
                        .unwrap_or(header.ordinal_table_offset);
                    (*chunk_start, chunk_end)
                })
                .collect::<Vec<_>>();
            let next_chunk = chunks.len();
            (
                RUN_HEADER_LEN as u64,
                RunKeyDirection::Reverse {
                    chunks,
                    next_chunk,
                    keys: Vec::new().into_iter(),
                },
            )
        } else {
            (
                sparse_start_for_key(&mut file, &header, start)?,
                RunKeyDirection::Forward,
            )
        };

        Ok(Self {
            file,
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
        while self.cursor < self.data_end {
            self.file.seek(SeekFrom::Start(self.cursor))?;
            let (key, next_cursor) = read_op_primary_key_frame(&mut self.file)?;
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
        let (chunk_start, chunk_end) = {
            let RunKeyDirection::Reverse {
                chunks, next_chunk, ..
            } = &mut self.direction
            else {
                return Ok(false);
            };
            if *next_chunk == 0 {
                self.exhausted = true;
                return Ok(false);
            }
            *next_chunk -= 1;
            chunks[*next_chunk]
        };
        let mut cursor = chunk_start;
        let mut chunk_keys: Vec<Bytes> = Vec::new();
        while cursor < chunk_end {
            self.file.seek(SeekFrom::Start(cursor))?;
            let (key, next_cursor) = read_op_primary_key_frame(&mut self.file)?;
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
