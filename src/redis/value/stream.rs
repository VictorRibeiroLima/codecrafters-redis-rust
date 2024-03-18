use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct StreamData {
    pub id: (u64, u64),
    pub fields: HashMap<String, String>,
}
