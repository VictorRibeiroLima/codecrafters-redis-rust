use std::collections::{HashMap, HashSet};

use self::{replication::Replication, value::Value};

mod replication;
pub mod types;
mod value;

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Redis {
    port: u16,
    memory: HashMap<String, Value>,
    keys: HashSet<String>,
    replication: Replication,
}

impl Redis {
    pub fn new(port: u16, replica_of: Option<(String, u16)>) -> Self {
        Self {
            port,
            replication: Replication::new(replica_of),
            ..Default::default()
        }
    }

    pub fn set(&mut self, key: String, value: String, expiration: Option<u128>) {
        let value = Value::new(value, expiration);
        self.memory.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        match self.memory.get(key) {
            Some(value) => {
                if value.is_expired() {
                    None
                } else {
                    Some(&value.value)
                }
            }
            None => None,
        }
    }

    pub fn expire_keys(&mut self) {
        let expired_keys: Vec<String> = self
            .memory
            .iter()
            .filter_map(|(key, value)| {
                if value.is_expired() {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();
        for key in expired_keys {
            self.memory.remove(&key);
            self.keys.remove(&key);
        }
    }

    pub fn replication_info(&self) -> String {
        self.replication.to_string()
    }
}
