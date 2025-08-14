use tokio::{io::AsyncWrite, sync::RwLock};

use crate::redis::{replication::RWStream, types::RedisType, value::ValueType, Redis};
use tokio::io::AsyncWriteExt;

use super::{CommandReturn, Handler, HandlerParams};

enum IdType {
    Id(u64, u64),
    Last,
}

impl IdType {
    async fn to_id<S: RWStream>(self, key: &str, redis: &RwLock<Redis<S>>) -> (u64, u64) {
        match self {
            IdType::Id(first, second) => (first, second),
            IdType::Last => {
                let redis_w = redis.read().await;
                let response = redis_w.get(key);
                let value = match response {
                    Some(value) => value,
                    None => return (0, 0),
                };
                let value = match &value.value {
                    ValueType::Stream(value) => value,
                    _ => return (0, 0),
                };
                let last = value.last();
                if last.is_none() {
                    return (0, 0);
                }
                let last = last.unwrap();
                let id = last.id;
                (id.0, id.1)
            }
        }
    }
}

pub struct XReadHandler {}

impl Handler for XReadHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: HandlerParams<'a, W, S>,
    ) -> CommandReturn {
        let args = params.args;
        let mut writer = params.writer;
        let should_reply = params.should_reply;
        let redis = params.redis;
        let mut iter = args.iter();
        let mut count = None;
        let mut blocks = None;
        let mut streams = Vec::new();
        let mut ids = Vec::new();

        //Map optional arguments
        while let Some(arg) = iter.next() {
            if arg.to_uppercase() == "COUNT" {
                let n_count = iter.next();
                if n_count.is_none() {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let response = RedisType::SimpleError(
                        "ERR wrong number of arguments for 'xread' command".to_string(),
                    );
                    let _ = writer.write_all(&response.encode()).await;
                    return CommandReturn::Error;
                }
                let n_count = n_count.unwrap();
                let n_count = match n_count.parse::<usize>() {
                    Ok(n) => n,
                    Err(_) => {
                        if !should_reply {
                            return CommandReturn::Error;
                        }
                        let response = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xread' command".to_string(),
                        );
                        let _ = writer.write_all(&response.encode()).await;
                        return CommandReturn::Error;
                    }
                };
                count = Some(n_count);
            }
            if arg.to_uppercase() == "BLOCK" {
                let n_blocks = iter.next();
                if n_blocks.is_none() {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let response = RedisType::SimpleError(
                        "ERR wrong number of arguments for 'xread' command".to_string(),
                    );
                    let _ = writer.write_all(&response.encode()).await;
                    return CommandReturn::Error;
                }
                let n_blocks = n_blocks.unwrap();
                let n_blocks = match n_blocks.parse::<u64>() {
                    Ok(n) => n,
                    Err(_) => {
                        if !should_reply {
                            return CommandReturn::Error;
                        }
                        let response = RedisType::SimpleError(
                            "ERR wrong number of arguments for 'xread' command".to_string(),
                        );
                        let _ = writer.write_all(&response.encode()).await;
                        return CommandReturn::Error;
                    }
                };
                blocks = Some(n_blocks);
            }
            if arg.to_uppercase() == "STREAMS" {
                break;
            }
        }

        //Map Streams
        while let Some(stream) = iter.next() {
            if stream.contains("-") || stream.parse::<u64>().is_ok() || stream == "$" {
                let id = str_to_id(stream);
                if id.is_err() {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let response = RedisType::SimpleError(
                        "ERR wrong number of arguments for 'xread' command".to_string(),
                    );
                    let _ = writer.write_all(&response.encode()).await;
                    return CommandReturn::Error;
                }
                ids.push(id.unwrap());
                break;
            }
            streams.push(stream);
        }

        if streams.is_empty() {
            if !should_reply {
                return CommandReturn::Error;
            }
            let response = RedisType::SimpleError(
                "ERR wrong number of arguments for 'xread' command".to_string(),
            );
            let _ = writer.write_all(&response.encode()).await;
            return CommandReturn::Error;
        }

        //Map IDs
        while let Some(id) = iter.next() {
            let id = str_to_id(id);
            if id.is_err() {
                if !should_reply {
                    return CommandReturn::Error;
                }
                let response = RedisType::SimpleError(
                    "ERR wrong number of arguments for 'xread' command".to_string(),
                );
                let _ = writer.write_all(&response.encode()).await;
                return CommandReturn::Error;
            }
            ids.push(id.unwrap());
        }

        if ids.len() != streams.len() {
            if !should_reply {
                return CommandReturn::Error;
            }
            let response = RedisType::SimpleError(
                "ERR wrong number of arguments for 'xread' command".to_string(),
            );
            let _ = writer.write_all(&response.encode()).await;
            return CommandReturn::Error;
        }

        let key_to_ids: Vec<(&&String, IdType)> = streams.iter().zip(ids.into_iter()).collect();
        let mut ids = vec![];
        for (key, id) in key_to_ids {
            let id = id.to_id(key, &redis).await;
            ids.push(id);
        }

        let redis_w = redis.read().await;
        let mut response = redis_w.get_x_read(&streams, &ids, count);
        drop(redis_w);
        if response == RedisType::NullArray {
            if let Some(blocks) = blocks {
                //start blocking
                let blocks = if blocks == 0 { u64::MAX } else { blocks };
                let mut now = std::time::SystemTime::now();
                let time_out = now
                    .checked_add(std::time::Duration::from_millis(blocks))
                    .unwrap();
                while now < time_out {
                    let redis_w = redis.read().await;
                    response = redis_w.get_x_read(&streams, &ids, count);
                    if response != RedisType::NullArray {
                        break;
                    }
                    //Careful with deadlocks
                    drop(redis_w);
                    now = std::time::SystemTime::now();
                }
            }
        }
        if should_reply {
            let _ = writer.write_all(&response.encode()).await;
        }
        CommandReturn::Ok
    }
}

fn str_to_id(s: &str) -> Result<IdType, ()> {
    if s.contains("-") {
        let mut iter = s.split("-");
        let first = iter.next().ok_or(())?.parse().map_err(|_| ())?;
        let second = iter.next().ok_or(())?.parse().map_err(|_| ())?;
        let id = IdType::Id(first, second);
        Ok(id)
    } else if s == "$" {
        Ok(IdType::Last)
    } else {
        let first = s.parse().map_err(|_| ())?;
        let id = IdType::Id(first, 0);
        Ok(id)
    }
}
