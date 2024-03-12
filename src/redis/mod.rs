use std::collections::HashMap;

pub struct Redis {
    memory: HashMap<String, String>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            memory: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.memory.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.memory.get(key)
    }
}
