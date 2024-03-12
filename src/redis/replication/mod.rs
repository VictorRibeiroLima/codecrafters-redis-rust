use std::fmt::Display;

use self::role::Role;

mod role;

#[derive(Debug, PartialEq, Default)]
pub struct Replication {
    pub role: Role,
    pub connected_slaves: usize,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<u64>,
    pub second_repl_offset: Option<i64>,
    pub repl_backlog_active: Option<u64>,
    pub repl_backlog_size: Option<u64>,
    pub repl_backlog_first_byte_offset: Option<u64>,
    pub repl_backlog_histlen: Option<u64>,
}

impl Display for Replication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut bulk_string = String::new();
        let role = self.role.to_string();
        let role_len = 5 + role.len();
        bulk_string.push_str(&format!("${}\r\nrole:{}\r\n", role_len, role));
        return write!(f, "{}", bulk_string);

        let role = self.role.to_string();
        let role_len = 5 + role.len();
        bulk_string.push_str(&format!("${}\r\nrole:{}\r\n", role_len, role));

        let con_s_str = self.connected_slaves.to_string();
        let con_s_str_len = 17 + con_s_str.len();
        bulk_string.push_str(&format!(
            "${}\r\nconnected_slaves:{}\r\n",
            con_s_str_len, con_s_str
        ));

        let master_replid = match &self.master_replid {
            Some(master_replid) => master_replid,
            None => "",
        };
        let master_replid_len = 14 + master_replid.len();
        bulk_string.push_str(&format!(
            "${}\r\nmaster_replid:{}\r\n",
            master_replid_len, master_replid
        ));

        let master_repl_offset = match &self.master_repl_offset {
            Some(master_repl_offset) => master_repl_offset.to_string(),
            None => "".to_string(),
        };
        let master_repl_offset_len = 20 + master_repl_offset.len();
        bulk_string.push_str(&format!(
            "${}\r\nmaster_repl_offset:{}\r\n",
            master_repl_offset_len, master_repl_offset
        ));

        let second_repl_offset = match &self.second_repl_offset {
            Some(second_repl_offset) => second_repl_offset.to_string(),
            None => "".to_string(),
        };
        let second_repl_offset_len = 20 + second_repl_offset.len();
        bulk_string.push_str(&format!(
            "${}\r\nsecond_repl_offset:{}\r\n",
            second_repl_offset_len, second_repl_offset
        ));

        let repl_backlog_active = match &self.repl_backlog_active {
            Some(repl_backlog_active) => repl_backlog_active.to_string(),
            None => "".to_string(),
        };
        let repl_backlog_active_len = 20 + repl_backlog_active.len();
        bulk_string.push_str(&format!(
            "${}\r\nrepl_backlog_active:{}\r\n",
            repl_backlog_active_len, repl_backlog_active
        ));

        let repl_backlog_size = match &self.repl_backlog_size {
            Some(repl_backlog_size) => repl_backlog_size.to_string(),
            None => "".to_string(),
        };
        let repl_backlog_size_len = 18 + repl_backlog_size.len();
        bulk_string.push_str(&format!(
            "${}\r\nrepl_backlog_size:{}\r\n",
            repl_backlog_size_len, repl_backlog_size
        ));

        let repl_backlog_first_byte_offset = match &self.repl_backlog_first_byte_offset {
            Some(repl_backlog_first_byte_offset) => repl_backlog_first_byte_offset.to_string(),
            None => "".to_string(),
        };
        let repl_backlog_first_byte_offset_len = 31 + repl_backlog_first_byte_offset.len();
        bulk_string.push_str(&format!(
            "${}\r\nrepl_backlog_first_byte_offset:{}\r\n",
            repl_backlog_first_byte_offset_len, repl_backlog_first_byte_offset
        ));

        let repl_backlog_histlen = match &self.repl_backlog_histlen {
            Some(repl_backlog_histlen) => repl_backlog_histlen.to_string(),
            None => "".to_string(),
        };
        let repl_backlog_histlen_len = 23 + repl_backlog_histlen.len();
        bulk_string.push_str(&format!(
            "${}\r\nrepl_backlog_histlen:{}\r\n",
            repl_backlog_histlen_len, repl_backlog_histlen
        ));

        write!(f, "{}", bulk_string)
    }
}
