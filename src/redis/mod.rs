use std::{
    collections::{HashMap, HashSet},
    string,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use bytes::{Buf, Bytes};

use self::{
    config::Config,
    replication::{role::Role, Replication},
    types::RedisType,
    value::Value,
};

pub mod config;
pub mod replication;
pub mod types;
mod value;

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Redis {
    magic_number: [u8; 5],
    version: [u8; 4],
    table_size: u32,
    expiry_size: u32,
    memory: HashMap<String, Value>,
    keys: HashSet<String>,
    pub replication: Replication,
    pub config: Config,
}

impl Redis {
    pub fn new(config: Config) -> Self {
        let mut redis = Self {
            replication: Replication::new(config.replica_of.clone()),
            config,
            ..Default::default()
        };

        if redis.config.dir.is_some() && redis.config.db_file_name.is_some() {
            let dir = redis.config.dir.clone().unwrap();
            let file = redis.config.db_file_name.clone().unwrap();
            redis.re_config(&dir, &file);
        }
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

    pub fn is_master(&self) -> bool {
        self.replication.role == Role::Master
    }

    pub async fn hand_shake(&self) -> Option<BufReader<TcpStream>> {
        if let Some((host, port)) = &self.replication.replica_of {
            //PING
            let stream = TcpStream::connect((host.clone(), *port))
                .await
                .expect("Failed to connect to master");
            let mut stream = BufReader::new(stream);
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
                RedisType::BulkString(self.config.port.to_string()),
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

            Some(stream)
        } else {
            None
        }
    }

    pub fn rdb_file_bytes(&self) -> Vec<u8> {
        self.gen_rdb_file()
    }

    pub fn get_keys(&self) -> RedisType {
        let mut arr = Vec::new();
        for key in &self.keys {
            arr.push(RedisType::BulkString(key.clone()));
        }
        RedisType::Array(arr)
    }

    fn gen_rdb_file(&self) -> Vec<u8> {
        let file = vec![
            82, 69, 68, 73, 83, 48, 48, 48, 57, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
            6, 54, 46, 48, 46, 49, 54, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115,
            192, 64, 250, 5, 99, 116, 105, 109, 101, 194, 31, 251, 241, 101, 250, 8, 117, 115, 101,
            100, 45, 109, 101, 109, 194, 240, 90, 12, 0, 250, 12, 97, 111, 102, 45, 112, 114, 101,
            97, 109, 98, 108, 101, 192, 0, 255, 245, 146, 4, 246, 233, 241, 232, 164,
        ];
        file
    }

    fn re_config(&mut self, dir: &str, file: &str) {
        let path = format!("{}/{}", dir, file);
        let file = std::fs::read(&path);
        let file = match file {
            Ok(file) => file,
            Err(_) => {
                println!("Failed to read file:{}", path);
                return;
            }
        };
        let mut file = Bytes::from(file);

        let magic_number: [u8; 5] = file.get(0..5).unwrap().try_into().unwrap();
        let version: [u8; 4] = file.get(5..9).unwrap().try_into().unwrap();

        for i in 9..file.len() {
            if file[i] == 0xFB {
                let _ = file.split_to(i);
                break;
            }
        }
        //Ignore the 0xFB byte
        let _ = file.get_u8();

        let length = file.get_u8();
        let table_length = encode_length(&mut file, length);

        let length = file.get_u8();
        let table_expiry_length = encode_length(&mut file, length);

        self.magic_number = magic_number;
        self.version = version;
        self.table_size = table_length;
        self.expiry_size = table_expiry_length;

        let mut new_memory = HashMap::new();
        let mut new_keys = HashSet::new();

        for _ in 0..table_length {
            let value_type = file.get_u8();
            if value_type == 0 {
                let key = encode_string(&mut file).unwrap();
                let value = encode_string(&mut file).unwrap();
                new_memory.insert(key.clone(), Value::new(value, None));
                new_keys.insert(key);
            }
        }

        self.memory = new_memory;
        self.keys = new_keys;
    }
}

//See:https://rdb.fnordig.de/file_format.html#length-encoded
fn encode_length(file: &mut Bytes, length: u8) -> u32 {
    match length {
        0b00000000..=0b00111111 => u32::from_be_bytes([0x00, 0x00, 0x00, length]),
        0b01000000..=0b01111111 => u32::from_be_bytes([0x00, 0x00, file.get_u8(), length]),
        0b10000000..=0b10111111 => file.get_u32(),
        0b11000000..=0b11111111 => 00,
    }
}

//See:https://rdb.fnordig.de/file_format.html#string-encoding
fn encode_string(file: &mut Bytes) -> Result<String, string::FromUtf8Error> {
    let length = file.get_u8();
    let key_length = encode_length(file, length);
    let i = file.split_to(key_length.try_into().unwrap()).to_vec();
    String::from_utf8(i)
}
