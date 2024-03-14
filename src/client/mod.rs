use std::sync::Arc;

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        RwLock,
    },
};

use crate::redis::{types::RedisType, Redis};

use self::command::{handle_command, Command};

mod command;

pub struct Client {
    pub stream: TcpStream,
    pub should_reply: bool,
    pub redis: Arc<RwLock<Redis>>,
}

impl Client {
    pub async fn handle_stream(&mut self) -> Result<()> {
        println!("Shoud reply: {}", self.should_reply);
        let mut buf = [0; 512];

        let (sender, mut receiver) = unbounded_channel::<Vec<u8>>();
        loop {
            /*This is a unnecessary hack,the "replication-11" test
            doesn't really creates a replica of the redis server
            it just pretends that it does, so we need to keep
            the cli client running, to pretend that it is a replica

            */
            tokio::select! {
                n = self.stream.read(&mut buf) => {
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

                    self.handle_command(&mut buf, n, sender.clone()).await?;
                }
                response = receiver.recv() => {
                    match response {
                        Some(response) => {
                            self.stream.write_all(&response).await?;
                            self.stream.flush().await?;
                        }
                        None => {
                            println!("No response");
                        }
                    }
                }
            }
        }
        println!("Client disconnected");
        Ok(())
    }

    async fn handle_command(
        &mut self,
        buff: &mut [u8; 512],
        n: usize,
        sender: UnboundedSender<Vec<u8>>,
    ) -> Result<()> {
        let buff = &buff[..n];
        let commands = match RedisType::from_buffer(buff) {
            Ok(c) => c,
            Err(_) => {
                println!("Assuming that this is the rbd file");
                //let e = "-ERR unknown command\r\n".to_string();
                //self.stream.write_all(e.as_bytes()).await?;
                return Ok(());
            }
        };

        for command in commands {
            let result = Command::split_type(command);
            let (command, args) = match result {
                Ok((c, a)) => (c, a),
                Err(_) => {
                    let e = "-ERR unknown command\r\n".to_string();
                    self.stream.write_all(e.as_bytes()).await?;
                    return Ok(());
                }
            };
            let (_, writer) = self.stream.split();
            handle_command(command, args, &self.redis, writer, sender.clone()).await;
        }
        Ok(())
    }
}

/*
fn parse_message<'a>(s: &'a str) -> Result<(Command, Vec<&'a str>), String> {
    println!("Parsing message: {:?}", s);
    let lines: Vec<&str> = s.lines().collect();

    let first_line = match lines.get(0) {
        Some(line) => *line,
        None => return Err("No message\r\n".to_string()),
    };

    if !first_line.contains("*") {
        return Err("Unknown message format\r\n".to_string());
    }

    let num_of_args = first_line
        .chars()
        .skip(1)
        .collect::<String>()
        .parse::<usize>()
        .map_err(|_| "Invalid number of arguments\r\n".to_string())?;

    let num_of_args = num_of_args * 2;

    if lines.len() - 1 != num_of_args {
        return Err("Invalid number of arguments\r\n".to_string());
    }

    let mut args: Vec<&str> = vec![];

    for i in 1..=num_of_args {
        if i % 2 != 0 {
            continue;
        }
        if lines[i].starts_with("$") {
            return Err("Invalid argument format\r\n".to_string());
        }
        let arg = lines[i];
        args.push(arg);
    }

    if args.len() == 0 {
        return Err("No command\r\n".to_string());
    }

    let command_str = args[0];

    let command = match Command::from_str(command_str) {
        Ok(c) => c,
        Err(_) => return Err("Unknown command\r\n".to_string()),
    };
    Ok((command, args[1..].to_vec()))
}

*/
