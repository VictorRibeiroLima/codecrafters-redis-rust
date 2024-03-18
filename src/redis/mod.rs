use std::{
    collections::{HashMap, HashSet},
    string,
    time::{Duration, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use bytes::{Buf, Bytes};

use self::{
    config::Config,
    replication::{role::Role, RWStream, Replication},
    types::RedisType,
    value::{Value, ValueType},
};

pub mod config;
pub mod replication;
pub mod types;
pub mod value;

#[derive(Debug)]
#[allow(dead_code)]
pub struct Redis<S: RWStream> {
    magic_number: [u8; 5],
    version: [u8; 4],
    table_size: u32,
    expiry_size: u32,
    memory: HashMap<String, Value>,
    keys: HashSet<String>,
    pub replication: Replication<S>,
    pub config: Config,
}

impl<S: RWStream> Redis<S> {
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

    pub fn set(&mut self, key: String, value: ValueType, expiration: Option<u64>) {
        let value = Value::new(value, expiration);
        self.memory.insert(key.clone(), value);
        self.keys.insert(key);
    }

    pub fn get(&self, key: &str) -> Option<&ValueType> {
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

    pub fn get_x_range(&self, key: &str, start: (u64, u64), end: (u64, u64)) -> RedisType {
        let value = self.memory.get(key);
        match value {
            Some(value) => match &value.value {
                ValueType::Stream(stream) => {
                    let mut result_vec = vec![];
                    let (first_start, second_start) = start;
                    let (fist_end, second_end) = end;
                    for data in stream {
                        let (first_id, second_id) = data.id;
                        let fist_in_range = first_id >= first_start && first_id <= fist_end;
                        let second_in_range = second_id >= second_start && second_id <= second_end;
                        if !fist_in_range || !second_in_range {
                            continue;
                        }

                        result_vec.push(data.into());
                    }
                    RedisType::Array(result_vec)
                }
                _ => RedisType::SimpleError("ERR wrong type of value".to_string()),
            },
            None => RedisType::NullArray,
        }
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut ValueType> {
        match self.memory.get_mut(key) {
            Some(value) => {
                if value.is_expired() {
                    None
                } else {
                    Some(&mut value.value)
                }
            }
            None => None,
        }
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.memory.remove(key);
        self.keys.remove(key)
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
            Err(e) => {
                println!("Failed to read file:{}", path);
                println!("Error:{}", e);
                return;
            }
        };
        let mut file = Bytes::from(file);

        let file_len = file.len();
        let magic_number: [u8; 5] = file.get(0..5).unwrap().try_into().unwrap();
        let version: [u8; 4] = file.get(5..9).unwrap().try_into().unwrap();

        let mut _oxfbi = 0;
        for i in 9..file_len {
            if file[i] == 0xFB {
                _oxfbi = i;
                break;
            }
        }
        if _oxfbi == 0 {
            return;
        }
        let _ = file.split_to(_oxfbi);
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
            let mut value_type = file.get_u8();
            let mut expiration = None;
            if value_type == 0xFD {
                let mut arr = vec![];
                for _ in 0..4 {
                    arr.push(file.get_u8());
                }
                let dur = u64::from_le_bytes(arr.try_into().unwrap());
                expiration = Some(UNIX_EPOCH + Duration::from_secs(dur));

                value_type = file.get_u8();
            } else if value_type == 0xFC {
                let mut arr = vec![];
                for _ in 0..8 {
                    arr.push(file.get_u8());
                }
                let dur = u64::from_le_bytes(arr.try_into().unwrap());
                let dur = dur / 1000;
                expiration = Some(UNIX_EPOCH + Duration::from_secs(dur));

                value_type = file.get_u8();
            }
            if value_type == 0 {
                let key = encode_string(&mut file).unwrap();
                let value = encode_string(&mut file).unwrap();
                let value = ValueType::String(value);
                new_memory.insert(key.clone(), Value::new_with_expiration(value, expiration));
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

impl<S: RWStream> Default for Redis<S> {
    fn default() -> Self {
        Self {
            magic_number: [0; 5],
            version: [0; 4],
            table_size: 0,
            expiry_size: 0,
            memory: HashMap::new(),
            keys: HashSet::new(),
            replication: Replication::new(None),
            config: Config::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::io::Mock;

    #[test]
    fn test_create_from_file() {
        let config = Config {
            db_file_name: Some("dump.rdb".to_string()),
            dir: Some(".".to_string()),
            port: 6379,
            replica_of: None,
        };

        let redis: Redis<Mock> = Redis::new(config);
        let keys = redis.keys.len();
        assert_eq!(keys, 4);
    }
}
