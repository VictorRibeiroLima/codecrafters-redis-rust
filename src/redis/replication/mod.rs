use std::fmt::Display;

use crate::redis::types::BulkString;

use self::role::Role;

mod role;

#[derive(Debug, PartialEq, Default)]
pub struct Replication {
    pub replica_of: Option<(String, u16)>,
    pub role: Role,
    pub connected_slaves: usize,
    pub master_replid: String,
    pub master_repl_offset: i32,
    pub second_repl_offset: i32,
    pub repl_backlog_active: i32,
    pub repl_backlog_size: i32,
    pub repl_backlog_first_byte_offset: i32,
    pub repl_backlog_histlen: i32,
}

impl Replication {
    pub fn new(replica_of: Option<(String, u16)>) -> Self {
        let role = match replica_of {
            Some(_) => Role::Slave,
            None => Role::Master,
        };

        Self {
            role,
            master_replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
            ..Default::default()
        }
    }
}

impl Display for Replication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        //return write!(f, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let mut bulk_string = BulkString::new();
        bulk_string.push("# Replication".to_string());
        let role = self.role.to_string();
        let role = format!("role:{}", role);
        bulk_string.push(role);

        let con_s_str = self.connected_slaves.to_string();
        let con_s_str = format!("connected_slaves:{}", con_s_str);
        bulk_string.push(con_s_str);

        let master_replid = &self.master_replid;
        let master_replid = format!("master_replid:{}", master_replid);
        bulk_string.push(master_replid);

        let master_repl_offset = &self.master_repl_offset;
        let master_repl_offset = format!("master_repl_offset:{}", master_repl_offset);
        bulk_string.push(master_repl_offset);

        let second_repl_offset = &self.second_repl_offset;
        let second_repl_offset = format!("second_repl_offset:{}", second_repl_offset);
        bulk_string.push(second_repl_offset);

        let repl_backlog_active = &self.repl_backlog_active;
        let repl_backlog_active = format!("repl_backlog_active:{}", repl_backlog_active);
        bulk_string.push(repl_backlog_active);

        let repl_backlog_size = &self.repl_backlog_size;
        let repl_backlog_size = format!("repl_backlog_size:{}", repl_backlog_size);
        bulk_string.push(repl_backlog_size);

        let repl_backlog_first_byte_offset = &self.repl_backlog_first_byte_offset;
        let repl_backlog_first_byte_offset = format!(
            "repl_backlog_first_byte_offset:{}",
            repl_backlog_first_byte_offset
        );
        bulk_string.push(repl_backlog_first_byte_offset);

        let repl_backlog_histlen = &self.repl_backlog_histlen;
        let repl_backlog_histlen = format!("repl_backlog_histlen:{}", repl_backlog_histlen);
        bulk_string.push(repl_backlog_histlen);

        let bulk_string = bulk_string.encode();

        write!(f, "{}", bulk_string)
    }
}
