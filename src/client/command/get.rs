use std::sync::Arc;

use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf, sync::RwLock};

use crate::redis::{types::RedisType, Redis};

use super::Handler;

pub struct GetHandler;

impl Handler for GetHandler {
    async fn handle<'a>(args: Vec<&str>, redis: &Arc<RwLock<Redis>>, stream: &mut WriteHalf<'a>) {
        let key = match args.get(0) {
            Some(key) => key.to_string(),
            None => {
                let response = RedisType::NullBulkString;
                let bytes = response.encode();
                let _ = stream.write_all(&bytes).await;
                return;
            }
        };
        let redis = redis.read().await;
        let response = match redis.get(&key) {
            Some(value) => RedisType::BulkString(value.to_string()),
            None => RedisType::NullBulkString,
        };
        let bytes = response.encode();
        let _ = stream.write_all(&bytes).await;
    }
}
