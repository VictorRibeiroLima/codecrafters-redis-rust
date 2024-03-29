use std::{fmt::Display, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
    time::timeout,
};

use crate::util;

use self::role::Role;

use super::types::RedisType;

pub mod role;

pub trait RWStream: AsyncWriteExt + AsyncReadExt + Unpin {}
#[derive(Debug)]
pub struct Replica<S: RWStream> {
    pub host: String,
    pub port: u16,
    pub stream: Mutex<BufReader<S>>,
}

#[derive(Debug)]
pub struct Replication<S: RWStream> {
    pub replica_of: Option<(String, u16)>,
    pub role: Role,
    pub connected_slaves: usize,
    pub master_replid: String,
    pub master_repl_offset: u64,
    pub second_repl_offset: i32,
    pub repl_backlog_active: i32,
    pub repl_backlog_size: i32,
    pub repl_backlog_first_byte_offset: i32,
    pub repl_backlog_histlen: i32,
    pub replicas: Vec<Replica<S>>,
    pub slave_read_repl_offset: u64,
}

impl<S: RWStream> Replication<S> {
    pub fn new(replica_of: Option<(String, u16)>) -> Self {
        let role = match replica_of {
            Some(_) => Role::Slave,
            None => Role::Master,
        };

        Self {
            role,
            master_replid: util::gen_rand_string(40),
            replica_of,
            ..Default::default()
        }
    }

    pub fn add_replica(&mut self, replica: Replica<S>) {
        self.connected_slaves += 1;
        self.replicas.push(replica);
    }

    pub async fn propagate_message(&mut self, message: Vec<u8>) {
        if self.role != Role::Master {
            return;
        }
        self.master_repl_offset += message.len() as u64;

        let mut remove = Vec::new();
        for (i, replica) in self.replicas.iter().enumerate() {
            let stream = &replica.stream;
            let mut stream = stream.lock().await;
            let response = stream.write_all(&message).await;
            if let Err(e) = response {
                println!("Failed to send message to replica: {}", e);
                self.connected_slaves -= 1;
                remove.push(i);
            }
        }

        for i in remove.iter().rev() {
            self.replicas.swap_remove(*i);
        }
    }

    pub async fn count_sync_replicas(&mut self, mut target: usize, time_limit: u64) -> usize {
        let mut now = std::time::SystemTime::now();
        let time_out = now
            .checked_add(std::time::Duration::from_millis(time_limit))
            .unwrap();

        let offset = self.master_repl_offset;
        let mut sync_replicas = 0;
        let command = RedisType::Array(vec![
            RedisType::BulkString("REPLCONF".to_string()),
            RedisType::BulkString("GETACK".to_string()),
            RedisType::BulkString("*".to_string()),
        ]);
        let command_len = command.len();
        let command = command.encode();
        let r_len = self.replicas.len();
        target = if target > r_len { r_len } else { target };
        println!("Counting sync replicas");
        println!("Target: {}", target);
        println!("Time limit: {}", time_limit);
        println!("Replicas: {}", r_len);
        println!("Offset: {}", offset);
        //All replicas are already in sync
        if offset == 0 {
            return r_len;
        }
        while now < time_out {
            for replica in &self.replicas {
                let stream = &replica.stream;
                let mut stream = stream.lock().await;
                let write_fut = stream.write_all(&command);
                let response = timeout(Duration::from_millis(1), write_fut).await;
                let response = match response {
                    Ok(r) => r,
                    Err(_) => {
                        println!("Failed to send message to replica: timeout");
                        continue;
                    }
                };
                if let Err(e) = response {
                    println!("Failed to send message to replica: {}", e);
                }
                let mut buffer = [0; 128];

                let read_fut = stream.read(&mut buffer);
                let response = timeout(Duration::from_millis(1), read_fut).await;
                let n = match response {
                    Ok(response) => {
                        let n = match response {
                            Ok(n) => n,
                            Err(e) => {
                                println!("Failed to read from replica: {}", e);
                                continue;
                            }
                        };
                        if n == 0 {
                            println!("Replica disconnected");
                            continue;
                        }
                        n
                    }
                    Err(_) => {
                        continue;
                    }
                };

                let buffer = &buffer[..n];

                let mut response = match RedisType::from_buffer(&buffer) {
                    Ok(r) => r,
                    Err(_) => {
                        println!("Buffer: {:?}", buffer);
                        println!("Failed to parse response from replica");
                        continue;
                    }
                };
                let response = match response.pop() {
                    Some(RedisType::Array(response)) => response,
                    _ => {
                        println!("Invalid response from replica");
                        continue;
                    }
                };
                let r_offset = match response.get(2) {
                    Some(RedisType::BulkString(offset)) => offset,
                    _ => {
                        println!("Offset not found in response from replica");
                        continue;
                    }
                };
                let r_offset = match r_offset.parse::<u64>() {
                    Ok(r_offset) => r_offset,
                    Err(_) => {
                        println!("Failed to parse offset from response from replica");
                        continue;
                    }
                };
                if r_offset >= offset {
                    sync_replicas += 1;
                } else {
                    println!("Replica offset: {}, Master offset: {}", r_offset, offset);
                }
            }
            self.master_repl_offset += command_len as u64;

            if sync_replicas >= target {
                break;
            }
            now = std::time::SystemTime::now();
        }

        return sync_replicas;
    }
}

impl<S: RWStream> Display for Replication<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        //return write!(f, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let mut bulk_string = String::new();
        bulk_string.push_str("# Replication\n");
        let role = self.role.to_string();
        let role = format!("role:{}\n", role);
        bulk_string.push_str(&role);

        let con_s_str = self.connected_slaves.to_string();
        let con_s_str = format!("connected_slaves:{}\n", con_s_str);
        bulk_string.push_str(&con_s_str);

        let master_replid = &self.master_replid;
        let master_replid = format!("master_replid:{}\n", master_replid);
        bulk_string.push_str(&master_replid);

        let master_repl_offset = &self.master_repl_offset;
        let master_repl_offset = format!("master_repl_offset:{}\n", master_repl_offset);
        bulk_string.push_str(&master_repl_offset);

        let second_repl_offset = &self.second_repl_offset;
        let second_repl_offset = format!("second_repl_offset:{}\n", second_repl_offset);
        bulk_string.push_str(&second_repl_offset);

        let repl_backlog_active = &self.repl_backlog_active;
        let repl_backlog_active = format!("repl_backlog_active:{}\n", repl_backlog_active);
        bulk_string.push_str(&repl_backlog_active);

        let repl_backlog_size = &self.repl_backlog_size;
        let repl_backlog_size = format!("repl_backlog_size:{}\n", repl_backlog_size);
        bulk_string.push_str(&repl_backlog_size);

        let repl_backlog_first_byte_offset = &self.repl_backlog_first_byte_offset;
        let repl_backlog_first_byte_offset = format!(
            "repl_backlog_first_byte_offset:{}\n",
            repl_backlog_first_byte_offset
        );
        bulk_string.push_str(&repl_backlog_first_byte_offset);

        let repl_backlog_histlen = &self.repl_backlog_histlen;
        let repl_backlog_histlen = format!("repl_backlog_histlen:{}", repl_backlog_histlen);
        bulk_string.push_str(&repl_backlog_histlen);

        write!(f, "{}", bulk_string)
    }
}

impl<S: RWStream> Default for Replication<S> {
    fn default() -> Self {
        Self {
            replica_of: Default::default(),
            role: Default::default(),
            connected_slaves: Default::default(),
            master_replid: Default::default(),
            master_repl_offset: Default::default(),
            second_repl_offset: Default::default(),
            repl_backlog_active: Default::default(),
            repl_backlog_size: Default::default(),
            repl_backlog_first_byte_offset: Default::default(),
            repl_backlog_histlen: Default::default(),
            replicas: Default::default(),
            slave_read_repl_offset: Default::default(),
        }
    }
}
