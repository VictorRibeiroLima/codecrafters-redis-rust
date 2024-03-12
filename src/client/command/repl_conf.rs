use std::sync::Arc;

use tokio::sync::RwLock;

use crate::redis::{types::RedisType, Redis};

pub async fn handle_repl_conf(args: Vec<&str>, _: &Arc<RwLock<Redis>>) -> RedisType {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match *arg {
            "listening-port" => {
                let _ = match iter.next() {
                    Some(port) => match port.parse::<u16>() {
                        Ok(port) => port,
                        Err(_) => return RedisType::Error("ERR invalid port".to_string()),
                    },
                    None => return RedisType::Error("ERR missing port".to_string()),
                };
                //let mut redis = redis.write().await;
                //redis.replication.listening_port = port;
                return RedisType::SimpleString("OK".to_string());
            }
            "capa" => {
                let _ = match iter.next() {
                    Some(capa) => capa,
                    None => return RedisType::Error("ERR missing capa".to_string()),
                };

                //let mut redis = redis.write().await;
                //redis.replication.capa = true;
                return RedisType::SimpleString("OK".to_string());
            }
            _ => {}
        }
    }
    todo!()
}
