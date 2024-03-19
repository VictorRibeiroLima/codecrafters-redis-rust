use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::redis::{
    replication::RWStream,
    types::RedisType,
    value::{stream::StreamData, ValueType},
};

use super::{Command, CommandReturn, Handler, HandlerParams};

pub struct XAddHandler;

impl Handler for XAddHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: HandlerParams<'a, W, S>,
    ) -> CommandReturn {
        let mut writer = params.writer;
        let should_reply = params.should_reply;
        let redis = params.redis;
        let args = params.args;

        //Key present
        let key = match args.get(0).cloned() {
            Some(key) => key,
            None => {
                if !should_reply {
                    return CommandReturn::Error;
                }
                let e = RedisType::SimpleError(
                    "ERR wrong number of arguments for 'xadd' command".to_string(),
                );
                let _ = writer.write_all(&e.encode()).await;
                return CommandReturn::Error;
            }
        };

        //ID present
        let id = match args.get(1).cloned() {
            Some(id) => id,
            None => {
                if !should_reply {
                    return CommandReturn::Error;
                }
                let e = RedisType::SimpleError(
                    "ERR wrong number of arguments for 'xadd' command".to_string(),
                );
                let _ = writer.write_all(&e.encode()).await;
                return CommandReturn::Error;
            }
        };

        let mut redis = redis.write().await;
        let value = redis.get_mut(&key);
        let last_value = match value {
            Some(ref v) => match v {
                ValueType::Stream(stream) => match stream.last() {
                    Some(last) => Some(last.id),
                    None => None,
                },
                _ => None,
            },
            None => None,
        };
        let key_id: (u64, u64);

        //ID generation
        if id == "*" {
            let first = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let second = 0;
            key_id = (first, second);
        } else {
            //ID parsing
            let slash_i = match id.find('-') {
                Some(i) => i,
                None => {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let e = RedisType::SimpleError(
                        "ERR wrong id format for 'xadd' command".to_string(),
                    );
                    let _ = writer.write_all(&e.encode()).await;
                    return CommandReturn::Error;
                }
            };

            let (first, second) = id.split_at(slash_i);
            let mut second_value = 1;
            let first_value = match first.parse::<u64>() {
                Ok(f) => f,
                Err(_) => {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let e = RedisType::SimpleError(
                        "ERR wrong id format for 'xadd' command".to_string(),
                    );
                    let _ = writer.write_all(&e.encode()).await;
                    return CommandReturn::Error;
                }
            };
            let second = &second[1..];

            //Partial ID generation
            if second == "*" {
                if let Some(last) = last_value {
                    if last.0 == first_value {
                        second_value = last.1 + 1;
                    } else if first_value > 0 {
                        second_value = 0;
                    }
                }
            } else {
                //ID parsing
                second_value = match second.parse::<u64>() {
                    Ok(s) => s,
                    Err(_) => {
                        if !should_reply {
                            return CommandReturn::Error;
                        }
                        let e = RedisType::SimpleError(
                            "ERR wrong id format for 'xadd' command".to_string(),
                        );
                        let _ = writer.write_all(&e.encode()).await;
                        return CommandReturn::Error;
                    }
                };
            }

            //ID validation
            if let Some(last) = last_value {
                if first_value == 0 && second_value == 0 {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let e = RedisType::SimpleError(
                        "ERR The ID specified in XADD must be greater than 0-0".to_string(),
                    );
                    let _ = writer.write_all(&e.encode()).await;
                    return CommandReturn::Error;
                }
                if first_value < last.0 {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let e = RedisType::SimpleError(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string(),
                    );
                    let _ = writer.write_all(&e.encode()).await;
                    return CommandReturn::Error;
                }
                if first_value == last.0 && second_value <= last.1 {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let e = RedisType::SimpleError(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string(),
                    );
                    let _ = writer.write_all(&e.encode()).await;
                    return CommandReturn::Error;
                }
            }
            key_id = (first_value, second_value);
        }

        let mut hash_map = HashMap::new();
        let mut i = 2;

        //Fields parsing
        while i < args.len() {
            let key = match args.get(i).cloned() {
                Some(f) => f,
                None => {
                    break;
                }
            };
            let value = match args.get(i + 1).cloned() {
                Some(v) => v,
                None => {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let e = RedisType::SimpleError(
                        "ERR wrong number of arguments for 'xadd' command".to_string(),
                    );
                    let _ = writer.write_all(&e.encode()).await;
                    return CommandReturn::Error;
                }
            };
            hash_map.insert(key, value);
            i += 2;
        }

        //Fields validation
        if hash_map.is_empty() {
            if !should_reply {
                return CommandReturn::Error;
            }
            let e = RedisType::SimpleError(
                "ERR wrong number of arguments for 'xadd' command".to_string(),
            );
            let _ = writer.write_all(&e.encode()).await;
            return CommandReturn::Error;
        }

        let stream = StreamData {
            id: key_id,
            fields: hash_map,
        };

        //Stream creation
        match value {
            Some(v) => match v {
                ValueType::Stream(v) => {
                    v.push(stream);
                }
                _ => {
                    if !should_reply {
                        return CommandReturn::Error;
                    }
                    let e = RedisType::SimpleError(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                    let _ = writer.write_all(&e.encode()).await;
                    return CommandReturn::Error;
                }
            },
            None => {
                let value = ValueType::Stream(vec![stream]);
                redis.set(key, value, None);
            }
        }

        //Replication
        let mut command: Vec<RedisType> = vec![Command::XAdd.into()];
        for arg in args {
            command.push(RedisType::BulkString(arg));
        }
        let command = RedisType::Array(command);
        redis.replication.propagate_message(command.encode()).await;

        if should_reply {
            let e = format!("{}-{}", key_id.0, key_id.1);
            let e = RedisType::BulkString(e);
            let _ = writer.write_all(&e.encode()).await;
        }
        return CommandReturn::Ok;
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::sync::RwLock;

    use tokio_test::io::Mock;

    use crate::redis::{
        config::Config,
        value::{stream::StreamData, ValueType},
        Redis,
    };
    use tokio_test::io::Builder;

    use crate::{client::command::Handler, redis::types::RedisType};

    #[tokio::test]
    async fn test_xadd_no_key() {
        let config = Config::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let response =
            RedisType::SimpleError("ERR wrong number of arguments for 'xadd' command".to_string());
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec![];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Error);
    }

    #[tokio::test]
    async fn test_xadd_no_id() {
        let config = Config::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let response =
            RedisType::SimpleError("ERR wrong number of arguments for 'xadd' command".to_string());
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec!["key".to_string()];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Error);
    }

    #[tokio::test]
    async fn test_xadd_no_fields() {
        let config = Config::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));
        let response =
            RedisType::SimpleError("ERR wrong number of arguments for 'xadd' command".to_string());
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec!["key".to_string(), "1-0".to_string()];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Error);
    }

    #[tokio::test]
    async fn test_xadd_invalid_ids() {
        let config = Config::default();
        let mut redis: Redis<Mock> = Redis::new(config);
        let stream = vec![
            StreamData {
                id: (1, 0),
                fields: vec![("field".to_string(), "value".to_string())]
                    .into_iter()
                    .collect(),
            },
            StreamData {
                id: (1, 1),
                fields: vec![("field".to_string(), "value".to_string())]
                    .into_iter()
                    .collect(),
            },
            StreamData {
                id: (2, 0),
                fields: vec![("field".to_string(), "value".to_string())]
                    .into_iter()
                    .collect(),
            },
            StreamData {
                id: (2, 10),
                fields: vec![("field".to_string(), "value".to_string())]
                    .into_iter()
                    .collect(),
            },
        ];
        redis.set("key".to_string(), ValueType::Stream(stream), None);
        let redis = Arc::new(RwLock::new(redis));

        //0-0 case
        let response = RedisType::SimpleError(
            "ERR The ID specified in XADD must be greater than 0-0".to_string(),
        );
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec![
            "key".to_string(),
            "0-0".to_string(),
            "field".to_string(),
            "value".to_string(),
        ];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Error);
        let redis_ref = redis.read().await;
        let values = redis_ref.get_value("key").unwrap();
        let stream = match values {
            ValueType::Stream(s) => s,
            _ => panic!(),
        };
        assert_eq!(stream.len(), 4);
        drop(redis_ref);

        //1-2 case (timestamp value is smaller than the last one)
        let response = RedisType::SimpleError(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        );
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec![
            "key".to_string(),
            "1-2".to_string(),
            "field".to_string(),
            "value".to_string(),
        ];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Error);
        let redis_ref = redis.read().await;
        let values = redis_ref.get_value("key").unwrap();
        let stream = match values {
            ValueType::Stream(s) => s,
            _ => panic!(),
        };
        assert_eq!(stream.len(), 4);
        drop(redis_ref);

        //2-0 case (timestamp value is equal to the last one, but sequence value is smaller)
        let response = RedisType::SimpleError(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        );
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec![
            "key".to_string(),
            "2-0".to_string(),
            "field".to_string(),
            "value".to_string(),
        ];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Error);
        let redis_ref = redis.read().await;
        let values = redis_ref.get_value("key").unwrap();
        let stream = match values {
            ValueType::Stream(s) => s,
            _ => panic!(),
        };
        assert_eq!(stream.len(), 4);
        drop(redis_ref);

        //1-* case (timestamp is smaller than the last one, but sequence value is gonna be generated)
        let response = RedisType::SimpleError(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        );
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec![
            "key".to_string(),
            "1-*".to_string(),
            "field".to_string(),
            "value".to_string(),
        ];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Error);
        let redis_ref = redis.read().await;
        let values = redis_ref.get_value("key").unwrap();
        let stream = match values {
            ValueType::Stream(s) => s,
            _ => panic!(),
        };
        assert_eq!(stream.len(), 4);
        drop(redis_ref);
    }

    #[tokio::test]
    async fn test_xadd() {
        let config = Config::default();
        let redis: Redis<Mock> = Redis::new(config);
        let redis = Arc::new(RwLock::new(redis));

        //fully qualified id case
        let response = RedisType::BulkString("1-0".to_string());
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec![
            "key".to_string(),
            "1-0".to_string(),
            "field".to_string(),
            "value".to_string(),
        ];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Ok);
        let redis_ref = redis.read().await;
        let values = redis_ref.get_value("key").unwrap();
        let stream = match values {
            ValueType::Stream(s) => s,
            _ => panic!(),
        };
        assert_eq!(stream.len(), 1);
        assert_eq!(stream[0].id, (1, 0));
        assert_eq!(stream[0].fields.len(), 1);
        assert_eq!(stream[0].fields.get("field").unwrap(), "value");
        drop(redis_ref);

        //timestamp only id case
        let response = RedisType::BulkString("2-0".to_string());
        let mut writer = Builder::new().write(&response.encode()).build();
        let args = vec![
            "key".to_string(),
            "2-*".to_string(),
            "field".to_string(),
            "value".to_string(),
        ];
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Ok);
        let redis_ref = redis.read().await;
        let values = redis_ref.get_value("key").unwrap();
        let stream = match values {
            ValueType::Stream(s) => s,
            _ => panic!(),
        };
        assert_eq!(stream.len(), 2);
        assert_eq!(stream[1].id, (2, 0));
        assert_eq!(stream[1].fields.len(), 1);
        assert_eq!(stream[1].fields.get("field").unwrap(), "value");
        drop(redis_ref);

        //Fully generated id case
        let args = vec![
            "key".to_string(),
            "*".to_string(),
            "field".to_string(),
            "value".to_string(),
        ];
        let ts: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let response = RedisType::BulkString(format!("{}-0", ts));
        let mut writer = Builder::new().write(&response.encode()).build();
        let params = super::HandlerParams {
            writer: &mut writer,
            should_reply: true,
            redis: &redis,
            args,
        };
        let result = super::XAddHandler::handle(params).await;
        assert_eq!(result, super::CommandReturn::Ok);
        let redis_ref = redis.read().await;
        let values = redis_ref.get_value("key").unwrap();
        let stream = match values {
            ValueType::Stream(s) => s,
            _ => panic!(),
        };
        assert_eq!(stream.len(), 3);
        assert_eq!(stream[2].id.0, ts);
        assert_eq!(stream[2].fields.len(), 1);
        assert_eq!(stream[2].fields.get("field").unwrap(), "value");
        drop(redis_ref);
    }
}
