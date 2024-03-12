use std::sync::Arc;

use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf, sync::RwLock};

use crate::redis::{types::RedisType, Redis};

use super::Handler;

pub struct EchoHandler;

impl Handler for EchoHandler {
    async fn handle<'a>(args: Vec<&str>, _redis: &Arc<RwLock<Redis>>, stream: &mut WriteHalf<'a>) {
        let first_arg = args.get(0).unwrap_or(&"");
        let str = format!("{}", first_arg);
        let response = RedisType::SimpleString(str);
        let _ = stream.write_all(&response.encode()).await;
    }
}
