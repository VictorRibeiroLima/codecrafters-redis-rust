use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

struct Value {
    value: String,
    _created_at: u128,
    expires_at: Option<u128>,
}
impl Value {
    fn new(value: String, expiration: Option<u128>) -> Self {
        let created_at = get_current_time();
        let expires_at = expiration.map(|expiration| created_at + expiration);
        Value {
            value,
            _created_at: created_at,
            expires_at,
        }
    }

    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => get_current_time() > expires_at,
            None => false,
        }
    }
}

pub struct Redis {
    memory: HashMap<String, Value>,
    keys: HashSet<String>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            memory: HashMap::new(),
            keys: HashSet::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String, expiration: Option<u128>) {
        let value = Value::new(value, expiration);
        self.memory.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.memory.get(key).map(|value| &value.value)
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
}

fn get_current_time() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
