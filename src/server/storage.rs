use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::storage::{Entry, Storage, StorageOp, StorageResult, StopSign};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// PaxosState — owns every field we need to persist.
// We use DeserializeOwned (= for<'de> Deserialize<'de>) on the type bounds
// instead of writing for<'de> Deserialize<'de> inline, which would shadow
// the 'de that #[derive(Deserialize)] introduces and cause E0496.
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
#[serde(bound = "T: Serialize + DeserializeOwned, T::Snapshot: Serialize + DeserializeOwned")]
struct PaxosState<T: Entry> {
    log: Vec<T>,
    n_prom: Option<Ballot>,
    acc_round: Option<Ballot>,
    ld: usize,
    trimmed_idx: usize,
    stopsign: Option<StopSign>,
    compacted_idx: usize,
    snapshot: Option<T::Snapshot>,
}

impl<T: Entry> Default for PaxosState<T> {
    fn default() -> Self {
        Self {
            log: vec![],
            n_prom: None,
            acc_round: None,
            ld: 0,
            trimmed_idx: 0,
            stopsign: None,
            compacted_idx: 0,
            snapshot: None,
        }
    }
}

// ---------------------------------------------------------------------------
// FileStorage — the public type used in server.rs
// ---------------------------------------------------------------------------

pub struct FileStorage<T: Entry> {
    state: PaxosState<T>,
    path: PathBuf,
}

impl<T: Entry + Serialize + DeserializeOwned> FileStorage<T>
where
    T::Snapshot: Serialize + DeserializeOwned,
{
    /// Load existing state from `path` if the file exists, otherwise start fresh.
    pub fn new(path: PathBuf) -> Self {
        let state = if path.exists() {
            let data = std::fs::read_to_string(&path)
                .expect("FileStorage: failed to read state file");
            serde_json::from_str(&data)
                .expect("FileStorage: failed to deserialize state file")
        } else {
            PaxosState::default()
        };
        Self { state, path }
    }

    /// Atomically flush state to disk using write-to-tmp + rename.
    /// rename(2) is atomic on Linux so a crash mid-write never corrupts the file.
    fn flush(&self) {
        let data = serde_json::to_string(&self.state)
            .expect("FileStorage: failed to serialize state");
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, &data)
            .expect("FileStorage: failed to write tmp file");
        std::fs::rename(&tmp, &self.path)
            .expect("FileStorage: failed to rename tmp -> state file");
    }
}

// ---------------------------------------------------------------------------
// Storage<T> implementation
// ---------------------------------------------------------------------------

impl<T: Entry + Serialize + DeserializeOwned + Clone> Storage<T> for FileStorage<T>
where
    T::Snapshot: Serialize + DeserializeOwned + Clone,
{
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        for op in ops {
            match op {
                StorageOp::AppendEntry(e) => {
                    self.state.log.push(e);
                }
                StorageOp::AppendEntries(mut es) => {
                    self.state.log.append(&mut es);
                }
                StorageOp::AppendOnPrefix(idx, es) => {
                    self.state.log.truncate(idx - self.state.trimmed_idx);
                    self.state.log.extend(es);
                }
                StorageOp::SetPromise(b) => {
                    self.state.n_prom = Some(b);
                }
                StorageOp::SetDecidedIndex(i) => {
                    self.state.ld = i;
                }
                StorageOp::SetAcceptedRound(b) => {
                    self.state.acc_round = Some(b);
                }
                // Compiler confirmed: SetStopsign (lowercase 's' in 'sign')
                StorageOp::SetStopsign(ss) => {
                    self.state.stopsign = ss;
                }
                StorageOp::SetCompactedIdx(i) => {
                    self.state.compacted_idx = i;
                }
                StorageOp::SetSnapshot(snap) => {
                    self.state.snapshot = snap;
                }
                #[allow(unreachable_patterns)]
                _ => {}
            }
        }
        self.flush();
        Ok(())
    }

    // ── Log entries ──────────────────────────────────────────────────────────

    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.state.log.push(entry);
        self.flush();
        Ok(())
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        self.state.log.extend(entries);
        self.flush();
        Ok(())
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        self.state.log.truncate(from_idx - self.state.trimmed_idx);
        self.state.log.extend(entries);
        self.flush();
        Ok(())
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        let from = from - self.state.trimmed_idx;
        let to = to - self.state.trimmed_idx;
        Ok(self.state.log.get(from..to).unwrap_or(&[]).to_vec())
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        Ok(self.state.log.len())
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        let from = from - self.state.trimmed_idx;
        Ok(self.state.log.get(from..).unwrap_or(&[]).to_vec())
    }

    fn trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let to_trim = (trimmed_idx - self.state.trimmed_idx).min(self.state.log.len());
        self.state.log.drain(0..to_trim);
        self.state.trimmed_idx = trimmed_idx;
        self.flush();
        Ok(())
    }

    // ── Ballot / round ───────────────────────────────────────────────────────

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.state.n_prom = Some(n_prom);
        self.flush();
        Ok(())
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.state.n_prom)
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.state.acc_round = Some(na);
        self.flush();
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.state.acc_round)
    }

    // ── Decided index ────────────────────────────────────────────────────────

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.state.ld = ld;
        self.flush();
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        Ok(self.state.ld)
    }

    // ── StopSign ─────────────────────────────────────────────────────────────

    fn set_stopsign(&mut self, ss: Option<StopSign>) -> StorageResult<()> {
        self.state.stopsign = ss;
        self.flush();
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        Ok(self.state.stopsign.clone())
    }

    // ── Compaction / snapshot ─────────────────────────────────────────────────

    fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.state.compacted_idx = idx;
        self.flush();
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        Ok(self.state.compacted_idx)
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.state.snapshot = snapshot;
        self.flush();
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        Ok(self.state.snapshot.clone())
    }
}