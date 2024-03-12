use std::sync::Arc;

use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf, sync::RwLock};

use crate::{
    redis::{types::RedisType, Redis},
    HOST,
};

use super::Handler;

pub struct ReplConfHandler;

impl Handler for ReplConfHandler {
    async fn handle<'a>(args: Vec<&str>, redis: &Arc<RwLock<Redis>>, writer: &mut WriteHalf<'a>) {
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            match *arg {
                "listening-port" => {
                    let port = match iter.next() {
                        Some(port) => match port.parse::<u16>() {
                            Ok(port) => port,
                            Err(_) => {
                                let response = RedisType::Error("ERR invalid port".to_string());
                                let bytes = response.encode();
                                let _ = writer.write_all(&bytes).await;
                                return;
                            }
                        },
                        None => {
                            let response = RedisType::Error("ERR missing port".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return;
                        }
                    };
                    let mut redis = redis.write().await;
                    redis.replication.add_replica((HOST.to_string(), port));
                    let response = RedisType::SimpleString("OK".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                }
                "capa" => {
                    let _ = match iter.next() {
                        Some(capa) => capa,
                        None => {
                            let response = RedisType::Error("ERR missing capa".to_string());
                            let bytes = response.encode();
                            let _ = writer.write_all(&bytes).await;
                            return;
                        }
                    };

                    let response = RedisType::SimpleString("OK".to_string());
                    let bytes = response.encode();
                    let _ = writer.write_all(&bytes).await;
                }
                _ => {}
            }
        }
    }
}
