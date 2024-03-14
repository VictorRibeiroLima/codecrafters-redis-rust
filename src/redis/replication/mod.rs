use std::fmt::Display;

use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{mpsc::UnboundedSender, Mutex},
};

use crate::util;

use self::role::Role;

pub mod role;

#[derive(Debug)]
pub struct Replica {
    pub host: String,
    pub port: u16,
    pub channel: UnboundedSender<Vec<u8>>,
    pub stream: Option<Mutex<TcpStream>>,
}

#[derive(Debug, Default)]
pub struct Replication {
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
    pub replicas: Vec<Replica>,
}

impl Replication {
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

    pub fn add_replica(&mut self, replica: Replica) {
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
            match &replica.stream {
                Some(stream) => {
                    let mut stream = stream.lock().await;
                    let response = stream.write_all(&message).await;
                    if let Err(e) = response {
                        println!("Failed to send message to replica: {}", e);
                        self.connected_slaves -= 1;
                        remove.push(i);
                    }
                }
                None => {
                    let channel = &replica.channel;
                    let r = channel.send(message.clone());
                    if let Err(e) = r {
                        println!("Failed to send message to replica: {}", e);
                    }
                }
            }
        }

        for i in remove.iter().rev() {
            self.replicas.swap_remove(*i);
        }
    }
}

impl Display for Replication {
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
