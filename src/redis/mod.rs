use std::collections::{HashMap, HashSet};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use self::{replication::Replication, types::RedisType, value::Value};

mod replication;
pub mod types;
mod value;

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Redis {
    port: u16,
    memory: HashMap<String, Value>,
    keys: HashSet<String>,
    pub replication: Replication,
}

impl Redis {
    pub async fn new(port: u16, replica_of: Option<(String, u16)>) -> Self {
        let redis = Self {
            port,
            replication: Replication::new(replica_of),
            ..Default::default()
        };
        redis.hand_shake().await;
        redis
    }

    pub fn set(&mut self, key: String, value: String, expiration: Option<u128>) {
        let value = Value::new(value, expiration);
        self.memory.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        match self.memory.get(key) {
            Some(value) => {
                if value.is_expired() {
                    None
                } else {
                    Some(&value.value)
                }
            }
            None => None,
        }
    }

    pub fn expire_keys(&mut self) {
        let expired_keys: Vec<String> = self
            .memory
            .iter()
            .filter_map(|(key, value)| {
                if value.is_expired() {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();
        for key in expired_keys {
            self.memory.remove(&key);
            self.keys.remove(&key);
        }
    }

    pub fn replication_info(&self) -> String {
        self.replication.to_string()
    }

    async fn hand_shake(&self) {
        if let Some((host, port)) = &self.replication.replica_of {
            //PING
            let mut stream = TcpStream::connect((host.clone(), *port))
                .await
                .expect("Failed to connect to master");
            let ping_command = RedisType::Array(vec![RedisType::BulkString("PING".to_string())]);
            let ping_command = ping_command.encode();
            stream
                .write_all(&ping_command)
                .await
                .expect("Failed to write to master");
            let mut buffer = [0; 128];
            stream
                .read(&mut buffer)
                .await
                .expect("Failed to read from master");

            //REPLCONF listening-port
            let command = RedisType::Array(vec![
                RedisType::BulkString("REPLCONF".to_string()),
                RedisType::BulkString("listening-port".to_string()),
                RedisType::BulkString(self.port.to_string()),
            ]);
            let command = command.encode();
            stream
                .write_all(&command)
                .await
                .expect("Failed to write to master");

            let mut buffer = [0; 128];
            stream
                .read(&mut buffer)
                .await
                .expect("Failed to read from master");

            //REPLCONF capa psync2
            let command = RedisType::Array(vec![
                RedisType::BulkString("REPLCONF".to_string()),
                RedisType::BulkString("capa".to_string()),
                RedisType::BulkString("psync2".to_string()),
            ]);
            let command = command.encode();
            stream
                .write_all(&command)
                .await
                .expect("Failed to write to master");

            let mut buffer = [0; 128];
            stream
                .read(&mut buffer)
                .await
                .expect("Failed to read from master");

            //PSYNC
            let command = RedisType::Array(vec![
                RedisType::BulkString("PSYNC".to_string()),
                RedisType::BulkString("?".to_string()),
                RedisType::BulkString("-1".to_string()),
            ]);
            let command = command.encode();
            stream
                .write_all(&command)
                .await
                .expect("Failed to write to master");

            let mut buffer = [0; 128];
            stream
                .read(&mut buffer)
                .await
                .expect("Failed to read from master");
        }
    }

    pub fn rdb_file_bytes(&self) -> Vec<u8> {
        //TODO: Implement RDB file generation
        let bytes = include_bytes!("../../dump.rdb");
        bytes.to_vec()
    }
}
