use std::sync::Arc;

use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf, sync::RwLock};

use crate::redis::{types::RedisType, Redis};

use super::Handler;

pub struct PingHandler;

impl Handler for PingHandler {
    async fn handle<'a>(_args: Vec<&str>, _redis: &Arc<RwLock<Redis>>, stream: &mut WriteHalf<'a>) {
        let response = RedisType::SimpleString("PONG".to_string());
        let bytes = response.encode();
        let _ = stream.write_all(&bytes).await;
    }
}
