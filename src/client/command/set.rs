use std::sync::Arc;

use tokio::{io::AsyncWriteExt, sync::RwLock};

use crate::redis::{types::RedisType, Redis};

pub struct SetHandler;

impl super::Handler for SetHandler {
    async fn handle<'a>(
        args: Vec<&str>,
        redis: &Arc<RwLock<Redis>>,
        writer: &mut tokio::net::tcp::WriteHalf<'a>,
    ) {
        let key = match args.get(0) {
            Some(key) => key.to_string(),
            None => {
                let response = RedisType::Error("ERR missing key".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return;
            }
        };
        let value = match args.get(1) {
            Some(value) => value.to_string(),
            None => {
                let response = RedisType::Error("ERR missing value".to_string());
                let bytes = response.encode();
                let _ = writer.write_all(&bytes).await;
                return;
            }
        };
        let expires_in = match args.get(2) {
            Some(expiration_command) => {
                let expiration_command = *expiration_command;
                if expiration_command != "px" {
                    {
                        let response = RedisType::Error("ERR invalid expiration".to_string());
                        let bytes = response.encode();
                        let _ = writer.write_all(&bytes).await;
                        return;
                    }
                }
                let expiration = match args.get(3) {
                    Some(expiration) => match expiration.parse::<u128>() {
                        Ok(expiration) => Some(expiration),
                        Err(_) => {
                            let response = RedisType::Error("ERR invalid expiration".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return;
                        }
                    },
                    None => {
                        let response = RedisType::Error("ERR invalid expiration".to_string());
                        let bytes = response.encode();
                        let _ = writer.write_all(&bytes).await;
                        return;
                    }
                };
                expiration
            }
            None => None,
        };
        let mut redis = redis.write().await;
        redis.set(key, value, expires_in);
        let response = RedisType::SimpleString("OK".to_string());
        let bytes = response.encode();
        let _ = writer.write_all(&bytes).await;
        redis.replication.propagate_message(bytes);
    }
}
