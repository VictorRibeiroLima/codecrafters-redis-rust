/*
server: General information about the Redis server
clients: Client connections section
memory: Memory consumption related information
persistence: RDB and AOF related information
stats: General statistics
replication: Master/replica replication information
cpu: CPU consumption statistics
commandstats: Redis command statistics
latencystats: Redis command latency percentile distribution statistics
sentinel: Redis Sentinel section (only applicable to Sentinel instances)
cluster: Redis Cluster section
modules: Modules section
keyspace: Database related statistics
errorstats: Redis error statistics
It can also take the following values:

all: Return all sections (excluding module generated ones)
default: Return only the default set of sections
everything: Includes all and modules
*/

use std::{str::FromStr, sync::Arc};

use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf, sync::RwLock};

use crate::redis::{types::RedisType, Redis};

use super::Handler;

#[derive(Debug, PartialEq)]
enum InfoCommand {
    SERVER,
    CLIENTS,
    MEMORY,
    PERSISTENCE,
    STATS,
    REPLICATION,
    CPU,
    COMMANDSTATS,
    LATENCYSTATS,
    SENTINEL,
    CLUSTER,
    MODULES,
    KEYSPACE,
    ERRORSTATS,
    ALL,
    DEFAULT,
    EVERYTHING,
}

impl FromStr for InfoCommand {
    type Err = RedisType;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SERVER" => Ok(InfoCommand::SERVER),
            "CLIENTS" => Ok(InfoCommand::CLIENTS),
            "MEMORY" => Ok(InfoCommand::MEMORY),
            "PERSISTENCE" => Ok(InfoCommand::PERSISTENCE),
            "STATS" => Ok(InfoCommand::STATS),
            "REPLICATION" => Ok(InfoCommand::REPLICATION),
            "CPU" => Ok(InfoCommand::CPU),
            "COMMANDSTATS" => Ok(InfoCommand::COMMANDSTATS),
            "LATENCYSTATS" => Ok(InfoCommand::LATENCYSTATS),
            "SENTINEL" => Ok(InfoCommand::SENTINEL),
            "CLUSTER" => Ok(InfoCommand::CLUSTER),
            "MODULES" => Ok(InfoCommand::MODULES),
            "KEYSPACE" => Ok(InfoCommand::KEYSPACE),
            "ERRORSTATS" => Ok(InfoCommand::ERRORSTATS),
            "ALL" => Ok(InfoCommand::ALL),
            "DEFAULT" => Ok(InfoCommand::DEFAULT),
            "EVERYTHING" => Ok(InfoCommand::EVERYTHING),
            _ => Err(RedisType::SimpleString(
                "-ERR unknown info command".to_string(),
            )),
        }
    }
}

pub struct InfoHandler;

impl Handler for InfoHandler {
    async fn handle(args: Vec<&str>, redis: &Arc<RwLock<Redis>>, stream: &mut WriteHalf<'_>) {
        let mut response = String::new();
        let command_str = args.get(0).unwrap_or(&"default");
        let command = match InfoCommand::from_str(command_str) {
            Ok(command) => command,
            Err(e) => {
                let response = e.encode();
                let _ = stream.write_all(&response).await;
                return;
            }
        };
        let redis = redis.read().await;

        match command {
            InfoCommand::REPLICATION => {
                response.push_str(&redis.replication_info());
            }
            _ => {
                let response = RedisType::Error("ERR unknown info command".to_string());
                let bytes = response.encode();
                let _ = stream.write_all(&bytes).await;
                return;
            }
        };
        let response = RedisType::BulkString(response);
        let bytes = response.encode();
        let _ = stream.write_all(&bytes).await;
    }
}
