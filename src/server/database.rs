use omnipaxos_kv::common::{kv::KVCommand, messages::KVResult};
use std::collections::HashMap;

pub struct Database {
    db: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    /// Returns Some(KVResult) for operations that need to send a response back,
    /// None for fire-and-forget writes.
    pub fn handle_command(&mut self, command: KVCommand) -> Option<KVResult> {
        match command {
            KVCommand::Put(key, value) => {
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => {
                let val = self.db.get(&key).cloned();
                Some(KVResult::Value(val))
            }
            KVCommand::Cas(key, expected, new_value) => {
                let current = self.db.get(&key).cloned();
                if current == expected {
                    if expected.is_none() {
                        self.db.insert(key, new_value);
                    } else {
                        self.db.insert(key, new_value);
                    }
                    Some(KVResult::CasOk)
                } else {
                    Some(KVResult::CasFailed(current))
                }
            }
        }
    }
}
