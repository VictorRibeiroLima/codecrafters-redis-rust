use std::time::SystemTime;

use self::stream::StreamData;

use super::types::RedisType;

pub mod stream;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum ValueType {
    String(String),
    Stream(Vec<StreamData>),
}

#[derive(Debug)]
pub struct Value {
    pub value: ValueType,
    pub _created_at: SystemTime,
    pub expires_at: Option<SystemTime>,
}
impl Value {
    pub fn new(value: ValueType, expiration: Option<u64>) -> Self {
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

    pub fn new_with_expiration(value: ValueType, expires_at: Option<SystemTime>) -> Self {
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

impl Into<RedisType> for Value {
    fn into(self) -> RedisType {
        match self.value {
            ValueType::String(s) => RedisType::BulkString(s),
            ValueType::Stream(s) => {
                let mut result_vec = vec![];
                for stream in &s {
                    result_vec.push(stream.into());
                }
                RedisType::Array(result_vec)
            }
        }
    }
}
