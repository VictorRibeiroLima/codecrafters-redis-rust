#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub enum RedisType {
    SimpleString(String),
    Error(String),
    BulkString(String),
    NullBulkString,
    Integer(i64),
    Array(Vec<RedisType>),
    Bytes(Vec<u8>),
}
impl RedisType {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RedisType::SimpleString(value) => format!("+{value}\r\n").into_bytes(),
            RedisType::Error(value) => format!("-{value}\r\n").into_bytes(),
            RedisType::Integer(value) => format!(":{value}\r\n").into_bytes(),
            RedisType::BulkString(value) => {
                format!("${}\r\n{value}\r\n", value.chars().count()).into_bytes()
            }
            RedisType::NullBulkString => b"$-1\r\n".to_vec(),
            RedisType::Array(values) => {
                let mut result = format!("*{}\r\n", values.len()).into_bytes();
                result.extend(values.iter().flat_map(|v| v.encode()));
                result
            }
            RedisType::Bytes(value) => {
                let mut result = format!("${}\r\n", value.len()).into_bytes();
                result.extend(value);
                result
            }
        }
    }
}
