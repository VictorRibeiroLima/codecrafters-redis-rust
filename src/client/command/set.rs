use std::sync::Arc;

use tokio::sync::RwLock;

use crate::redis::Redis;

pub async fn handle_set(args: Vec<&str>, redis: &Arc<RwLock<Redis>>) -> String {
    let key = match args.get(0) {
        Some(key) => key.to_string(),
        None => return "-ERR missing key\r\n".to_string(),
    };
    let value = match args.get(1) {
        Some(value) => value.to_string(),
        None => return "-ERR missing value\r\n".to_string(),
    };
    let expires_in = match args.get(2) {
        Some(expiration_command) => {
            let expiration_command = *expiration_command;
            if expiration_command != "px" {
                return "-ERR unknown command\r\n".to_string();
            }
            let expiration = match args.get(3) {
                Some(expiration) => match expiration.parse::<u128>() {
                    Ok(expiration) => Some(expiration),
                    Err(_) => return "-ERR invalid expiration\r\n".to_string(),
                },
                None => return "-ERR missing expiration\r\n".to_string(),
            };
            expiration
        }
        None => None,
    };
    let mut redis = redis.write().await;
    redis.set(key, value, expires_in);
    "+OK\r\n".to_string()
}
