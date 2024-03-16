use std::time::SystemTime;

#[derive(Debug)]
pub struct Value {
    pub value: String,
    pub _created_at: SystemTime,
    pub expires_at: Option<SystemTime>,
}
impl Value {
    pub fn new(value: String, expiration: Option<u64>) -> Self {
        let created_at = SystemTime::now();
        let expires_at = match expiration {
            Some(expiration) => {
                let expires_at = created_at + std::time::Duration::from_millis(expiration);
                Some(expires_at)
            }
            None => None,
        };
        Value {
            value,
            _created_at: created_at,
            expires_at,
        }
    }

    pub fn new_with_expiration(value: String, expires_at: Option<SystemTime>) -> Self {
        Value {
            value,
            _created_at: SystemTime::now(),
            expires_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => SystemTime::now() > expires_at,
            None => false,
        }
    }
}
