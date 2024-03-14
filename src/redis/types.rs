use std::{fmt::Display, str::Lines};

#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub enum RedisType {
    SimpleString(String),
    SimpleError(String),
    BulkString(String),
    NullBulkString,
    Integer(i64),
    Array(Vec<RedisType>),
    Bytes(Vec<u8>),
}

impl RedisType {
    pub fn from_buffer(buffer: &[u8]) -> Result<Vec<RedisType>, ()> {
        let mut result = vec![];
        let str = std::str::from_utf8(buffer).map_err(|_| ())?;

        let mut lines = str.lines();

        while let Some(line) = lines.next() {
            if line.is_empty() {
                continue;
            }
            let first_char = line.chars().next().ok_or(())?;
            match first_char {
                '+' => {
                    let redis_type = Self::SimpleString(line[1..].to_string());
                    result.push(redis_type);
                }
                '-' => {
                    let redis_type = Self::SimpleError(line[1..].to_string());
                    result.push(redis_type);
                }
                ':' => {
                    let redis_type = Self::Integer(line[1..].parse().map_err(|_| ())?);
                    result.push(redis_type);
                }
                '$' => {
                    let len: i8 = line[1..].parse().map_err(|_| ())?;
                    if len == -1 {
                        result.push(Self::NullBulkString);
                    } else {
                        let value = lines.next().ok_or(())?;
                        result.push(Self::BulkString(value.to_string()));
                    }
                }
                '*' => {
                    let len: usize = line[1..].parse().map_err(|_| ())?;
                    let mut array = vec![];
                    for _ in 0..len {
                        let redis_type = Self::inner_from(&mut lines)?;
                        array.push(redis_type);
                    }
                    result.push(Self::Array(array));
                }
                _ => {
                    return Err(());
                }
            }
        }
        return Ok(result);
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            RedisType::SimpleString(value) => format!("+{value}\r\n").into_bytes(),
            RedisType::SimpleError(value) => format!("-{value}\r\n").into_bytes(),
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

    fn inner_from(lines: &mut Lines) -> Result<RedisType, ()> {
        let line = lines.next().ok_or(())?;
        let first_char = line.chars().next().ok_or(())?;
        match first_char {
            '+' => Ok(RedisType::SimpleString(line[1..].to_string())),
            '-' => Ok(RedisType::SimpleError(line[1..].to_string())),
            ':' => Ok(RedisType::Integer(line[1..].parse().map_err(|_| ())?)),
            '$' => {
                let len: i8 = line[1..].parse().map_err(|_| ())?;
                if len == -1 {
                    Ok(RedisType::NullBulkString)
                } else {
                    let value = lines.next().ok_or(())?;
                    Ok(RedisType::BulkString(value.to_string()))
                }
            }
            '*' => {
                let len: usize = line[1..].parse().map_err(|_| ())?;
                let mut array = vec![];
                for _ in 0..len {
                    let redis_type = Self::inner_from(lines)?;
                    array.push(redis_type);
                }
                Ok(RedisType::Array(array))
            }
            _ => Err(()),
        }
    }
}

impl Display for RedisType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisType::SimpleString(value) => write!(f, "{}", value),
            RedisType::SimpleError(value) => write!(f, "{}", value),
            RedisType::Integer(value) => write!(f, "{}", value),
            RedisType::BulkString(value) => write!(f, "{}", value),
            RedisType::NullBulkString => write!(f, "-1"),
            RedisType::Array(values) => {
                let mut result = format!("{}", values.len());
                for value in values {
                    result.push_str(&format!("{}", value));
                }
                write!(f, "{}", result)
            }
            RedisType::Bytes(_) => write!(f, "bytes",),
        }
    }
}
