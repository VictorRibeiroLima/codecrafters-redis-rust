use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::redis::{replication::RWStream, types::RedisType};

use super::{Command, CommandReturn, Handler, HandlerParams};

pub struct DelHandler;

impl Handler for DelHandler {
    async fn handle<'a, W: AsyncWrite + Unpin, S: RWStream>(
        params: HandlerParams<'a, W, S>,
    ) -> CommandReturn {
        let mut writer = params.writer;
        let args = params.args;
        let redis = params.redis;
        let mut del_count = 0;

        let mut redis = redis.write().await;
        let mut command: Vec<RedisType> = vec![Command::Del.into()];
        for arg in args.into_iter() {
            let deleted = redis.delete(&arg);
            command.push(RedisType::BulkString(arg));
            if deleted {
                del_count += 1;
            }
        }
        let command = RedisType::Array(command);
        if params.should_reply {
            let response = RedisType::Integer(del_count);
            let bytes = response.encode();
            let _ = writer.write_all(&bytes).await;
        }
        redis.replication.propagate_message(command.encode()).await;
        return CommandReturn::Ok;
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tokio_test::io::{Builder, Mock};

    use crate::{
        client::command::{del::DelHandler, CommandReturn, Handler, HandlerParams},
        redis::{config::Config, types::RedisType, value::ValueType, Redis},
    };

    #[tokio::test]
    async fn test_del() {
        let config = Config::default();
        let mut redis: Redis<Mock> = Redis::new(config);
        redis.set(
            "key1".to_string(),
            ValueType::String("value1".to_string()),
            None,
        );
        redis.set(
            "key2".to_string(),
            ValueType::String("value2".to_string()),
            None,
        );
        redis.set(
            "key3".to_string(),
            ValueType::String("value3".to_string()),
            None,
        );
        let redis = Arc::new(RwLock::new(redis));
        let response = RedisType::Integer(2);
        let mut mock = Builder::new().write(&response.encode()).build();
        let args = vec!["key1".to_string(), "key2".to_string()];
        let params = HandlerParams {
            args,
            redis: &redis.clone(),
            writer: &mut mock,
            should_reply: true,
        };
        let result = DelHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
        let redis = redis.read().await;
        assert_eq!(redis.get_value("key1"), None);
        assert_eq!(redis.get_value("key2"), None);
        assert_eq!(
            redis.get_value("key3"),
            Some(&ValueType::String("value3".to_string()))
        );
    }

    #[tokio::test]
    async fn test_del_no_reply() {
        let config = Config::default();
        let mut redis: Redis<Mock> = Redis::new(config);
        redis.set(
            "key1".to_string(),
            ValueType::String("value1".to_string()),
            None,
        );
        redis.set(
            "key2".to_string(),
            ValueType::String("value2".to_string()),
            None,
        );
        redis.set(
            "key3".to_string(),
            ValueType::String("value3".to_string()),
            None,
        );
        let redis = Arc::new(RwLock::new(redis));
        let mut mock = Builder::new().build();
        let args = vec!["key1".to_string(), "key2".to_string()];
        let params = HandlerParams {
            args,
            redis: &redis.clone(),
            writer: &mut mock,
            should_reply: false,
        };
        let result = DelHandler::handle(params).await;
        assert_eq!(result, CommandReturn::Ok);
        let redis = redis.read().await;
        assert_eq!(redis.get_value("key1"), None);
        assert_eq!(redis.get_value("key2"), None);
        assert_eq!(
            redis.get_value("key3"),
            Some(&ValueType::String("value3".to_string()))
        );
    }
}
