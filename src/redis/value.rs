use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Value {
    pub value: String,
    pub _created_at: u128,
    pub expires_at: Option<u128>,
}
impl Value {
    pub fn new(value: String, expiration: Option<u128>) -> Self {
        let created_at = get_current_time();
        let expires_at = expiration.map(|expiration| created_at + expiration);
        Value {
            value,
            _created_at: created_at,
            expires_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => get_current_time() > expires_at,
            None => false,
        }
    }
}

fn get_current_time() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
