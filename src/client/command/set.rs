use std::sync::Arc;

use tokio::sync::RwLock;

use crate::redis::{types::RedisType, Redis};

pub async fn handle_set(args: Vec<&str>, redis: &Arc<RwLock<Redis>>) -> RedisType {
    let key = match args.get(0) {
        Some(key) => key.to_string(),
        None => return RedisType::Error("ERR missing key".to_string()),
    };
    let value = match args.get(1) {
        Some(value) => value.to_string(),
        None => return RedisType::Error("ERR missing value".to_string()),
    };
    let expires_in = match args.get(2) {
        Some(expiration_command) => {
            let expiration_command = *expiration_command;
            if expiration_command != "px" {
                return RedisType::Error("ERR invalid expiration command".to_string());
            }
            let expiration = match args.get(3) {
                Some(expiration) => match expiration.parse::<u128>() {
                    Ok(expiration) => Some(expiration),
                    Err(_) => return RedisType::Error("ERR invalid expiration".to_string()),
                },
                None => return RedisType::Error("ERR missing expiration".to_string()),
            };
            expiration
        }
        None => None,
    };
    let mut redis = redis.write().await;
    redis.set(key, value, expires_in);
    RedisType::SimpleString("OK".to_string())
}
