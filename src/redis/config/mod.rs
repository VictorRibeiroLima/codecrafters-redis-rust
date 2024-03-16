use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use super::types::RedisType;

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub dir: Option<String>,
    pub db_file_name: Option<String>,
    pub port: u16,
    pub replica_of: Option<(String, u16)>,
}

impl Config {
    pub fn get_value(&self, key: &str) -> Result<RedisType, ()> {
        if key == "*" {
            return Ok(self.get_all());
        }
        let key = ConfigKey::from_str(key).map_err(|_| ())?;
        let key_s = key.to_string();
        let value = self.inner_get_value(key);

        return Ok(RedisType::Array(vec![RedisType::BulkString(key_s), value]));
    }

    fn get_all(&self) -> RedisType {
        let result = RedisType::Array(vec![
            RedisType::BulkString("dir".to_string()),
            self.inner_get_value(ConfigKey::Dir),
            RedisType::BulkString("dbfilename".to_string()),
            self.inner_get_value(ConfigKey::DbFileName),
            RedisType::BulkString("port".to_string()),
            self.inner_get_value(ConfigKey::Port),
            RedisType::BulkString("replicaof".to_string()),
            self.inner_get_value(ConfigKey::ReplicaOf),
        ]);
        result
    }

    fn inner_get_value(&self, key: ConfigKey) -> RedisType {
        let value = match key {
            ConfigKey::Dir => self.dir.clone(),
            ConfigKey::DbFileName => self.db_file_name.clone(),
            ConfigKey::Port => Some(self.port.to_string()),
            ConfigKey::ReplicaOf => self
                .replica_of
                .as_ref()
                .map(|(host, port)| format!("{}:{}", host, port)),
        };
        match value {
            Some(value) => RedisType::BulkString(value),
            None => RedisType::NullBulkString,
        }
    }
}

pub enum ConfigKey {
    Dir,
    DbFileName,
    Port,
    ReplicaOf,
}

impl FromStr for ConfigKey {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "dir" => Ok(ConfigKey::Dir),
            "dbfilename" => Ok(ConfigKey::DbFileName),
            "port" => Ok(ConfigKey::Port),
            "replicaof" => Ok(ConfigKey::ReplicaOf),
            _ => Err(()),
        }
    }
}

impl Display for ConfigKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigKey::Dir => write!(f, "dir"),
            ConfigKey::DbFileName => write!(f, "dbfilename"),
            ConfigKey::Port => write!(f, "port"),
            ConfigKey::ReplicaOf => write!(f, "replicaof"),
        }
    }
}
