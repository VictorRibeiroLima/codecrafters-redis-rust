use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{Mutex, RwLock},
};

use crate::{
    redis::{replication::Replica, types::RedisType, Redis},
    HOST,
};

use self::command::{handle_command, Command, CommandReturn};

mod command;

pub struct Client {
    pub stream: BufReader<TcpStream>,
    pub should_reply: bool,
    pub redis: Arc<RwLock<Redis>>,
    pub addr: Option<SocketAddr>,
}

impl Client {
    pub async fn handle_stream(mut self) -> Result<()> {
        println!("Shoud reply: {}", self.should_reply);
        let mut buf = [0; 512];

        loop {
            let n = self.stream.read(&mut buf).await;

            let n = match n {
                Ok(n) => n,
                Err(e) => {
                    println!("Failed to read from socket; err = {:?}", e);
                    return Ok(());
                }
            };
            if n == 0 {
                break;
            }

            let commands = self.get_commands(&mut buf, n)?;
            let should_reply = self.should_reply;
            for command in commands {
                let command_len = command.len();
                let result = Command::split_type(command);
                let (command, args) = match result {
                    Ok((c, a)) => (c, a),
                    Err(_) => {
                        if !should_reply {
                            continue;
                        }
                        let e = "-ERR unknown command\r\n".to_string();
                        self.stream.write_all(e.as_bytes()).await?;
                        continue;
                    }
                };
                let (_, writer) = self.stream.get_mut().split();
                let c_return =
                    handle_command(command, args, &self.redis, writer, self.should_reply).await;
                /*This is a unnecessary hack,the "replication-11" test
                doesn't really creates a replica of the redis server
                it just pretends that it does, so we need to keep
                the cli client running, to pretend that it is a replica

                */
                match c_return {
                    CommandReturn::ConsumeTcpStream => {
                        if self.addr.is_none() {
                            continue;
                        }
                        let mut redis = self.redis.write().await;
                        let port = self.addr.as_ref().unwrap().port();
                        let replica = Replica {
                            host: HOST.to_string(),
                            port,
                            stream: None,
                        };
                        redis.replication.add_replica(replica);
                    }
                    CommandReturn::HandShakeCompleted => {
                        if self.addr.is_none() {
                            continue;
                        }
                        let mut redis = self.redis.write().await;
                        let host = HOST;
                        let port = self.addr.unwrap().port();
                        let replica = redis
                            .replication
                            .find_replica_mut(&host, port)
                            .expect("Replica not found");
                        replica.stream = Some(Mutex::new(self.stream.into_inner()));
                        return Ok(());
                    }
                    _ => {}
                }
                if !should_reply {
                    let mut redis = self.redis.write().await;
                    redis.replication.slave_read_repl_offset += command_len as u64;
                }
            }
        }
        println!("Client disconnected");
        Ok(())
    }

    fn get_commands(&mut self, buff: &mut [u8; 512], n: usize) -> Result<Vec<RedisType>> {
        let buff: &[u8] = &buff[..n];
        let commands = match RedisType::from_buffer(buff) {
            Ok(c) => c,
            Err(_) => return Err(anyhow::Error::msg("")),
        };

        return Ok(commands);
    }
}
