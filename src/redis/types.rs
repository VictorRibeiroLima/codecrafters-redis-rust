use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub enum RedisType {
    SimpleString(String),
    SimpleError(String),
    BulkString(String),
    NullBulkString,
    Integer(i64),
    Array(Vec<RedisType>),
    NullArray,
    Bytes(Vec<u8>),
}

impl RedisType {
    pub fn from_buffer(buffer: &[u8]) -> Result<Vec<RedisType>, ()> {
        //println!("buffer: {:?}", buffer);
        let mut result = vec![];
        Self::inner_from_buffer(buffer, &mut result)?;
        Ok(result)
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
            RedisType::NullArray => b"*-1\r\n".to_vec(),
            RedisType::Bytes(value) => {
                let mut result = format!("${}\r\n", value.len()).into_bytes();
                result.extend(value);
                result
            }
        }
    }

    fn inner_from_buffer(buffer: &[u8], result: &mut Vec<RedisType>) -> Result<(), ()> {
        if buffer.is_empty() {
            return Ok(());
        }
        let first_char = buffer[0] as char;
        let mut i;
        match first_char {
            '$' => {
                let (j, redis_type) = Self::map_dollar(buffer)?;
                result.push(redis_type);
                i = j;
            }
            '+' => {
                let (j, redis_type) = Self::map_plus(buffer)?;
                result.push(redis_type);
                i = j;
            }
            '-' => {
                let (j, redis_type) = Self::map_minus(buffer)?;
                result.push(redis_type);
                i = j;
            }
            ':' => {
                let (j, redis_type) = Self::map_colon(buffer)?;
                result.push(redis_type);
                i = j;
            }
            '*' => {
                let mut j = 1;
                let mut len: i32 = 0;
                let mut negative = false;
                loop {
                    let next_char = buffer[j] as char;
                    if next_char == '\r' && buffer.get(j + 1) == Some(&b'\n') {
                        i = j + 2;
                        break;
                    }
                    if next_char == '-' {
                        negative = true;
                    } else {
                        let next_char = next_char.to_digit(10).ok_or(())? as i32;
                        if negative {
                            len = len * 10 - next_char;
                        } else {
                            len = len * 10 + next_char;
                        }
                    }
                    j += 1;
                }
                if len == -1 {
                    result.push(Self::NullArray);
                } else {
                    let mut inner_result = vec![];
                    let mut other_i = j;
                    Self::map_star(&buffer[i..], len as usize, &mut other_i, &mut inner_result)?;
                    result.push(Self::Array(inner_result));
                    i += other_i - 2;
                }
            }
            _ => {
                return Err(());
            }
        };

        let buffer_len = buffer.len();
        if i >= buffer_len {
            return Ok(());
        }
        return Self::inner_from_buffer(&buffer[i..], result);
    }

    fn map_star(
        mut buffer: &[u8],
        len: usize,
        other_i: &mut usize,
        result: &mut Vec<RedisType>,
    ) -> Result<(), ()> {
        if buffer.is_empty() {
            return Ok(());
        }

        let mut i;
        for _ in 0..len {
            let first_char = buffer[0] as char;
            match first_char {
                '$' => {
                    let (j, redis_type) = Self::map_dollar(buffer)?;
                    result.push(redis_type);
                    i = j;
                }
                '+' => {
                    let (j, redis_type) = Self::map_plus(buffer)?;
                    result.push(redis_type);
                    i = j;
                }
                '-' => {
                    let (j, redis_type) = Self::map_minus(buffer)?;
                    result.push(redis_type);
                    i = j;
                }
                ':' => {
                    let (j, redis_type) = Self::map_colon(buffer)?;
                    result.push(redis_type);
                    i = j;
                }
                '*' => {
                    let mut j = 1;
                    let mut len: i32 = 0;
                    let mut negative = false;
                    loop {
                        let next_char = buffer[j] as char;
                        if next_char == '\r' && buffer.get(j + 1) == Some(&b'\n') {
                            i = j + 2;
                            break;
                        }
                        if next_char == '-' {
                            negative = true;
                        } else {
                            let next_char = next_char.to_digit(10).ok_or(())? as i32;
                            if negative {
                                len = len * 10 - next_char;
                            } else {
                                len = len * 10 + next_char;
                            }
                        }
                        j += 1;
                    }
                    if len == -1 {
                        result.push(Self::NullArray);
                    } else {
                        let mut inner_result = vec![];
                        let mut other_i = j;
                        Self::map_star(
                            &buffer[i..],
                            len as usize,
                            &mut other_i,
                            &mut inner_result,
                        )?;
                        result.push(Self::Array(inner_result));
                        i += other_i - 2;
                    }
                }
                _ => {
                    return Err(());
                }
            }
            *other_i += i;
            buffer = &buffer[i..];
        }

        Ok(())
    }

    fn map_dollar(buffer: &[u8]) -> Result<(usize, RedisType), ()> {
        let mut len: i32 = 0;
        let mut i = 1;
        let mut negative = false;
        loop {
            let nex_val = match buffer.get(i) {
                Some(val) => val,
                None => {
                    break;
                }
            };
            let next_char = *nex_val as char;
            if next_char == '\r' && buffer.get(i + 1) == Some(&b'\n') {
                i += 2;
                break;
            }
            if next_char == '-' {
                negative = true;
            } else {
                let next_char = next_char.to_digit(10).ok_or(())? as i32;
                if negative {
                    len = len * 10 - next_char;
                } else {
                    len = len * 10 + next_char;
                }
            }
            i += 1;
        }
        if len == -1 {
            return Ok((i, Self::NullBulkString));
        } else {
            let len = len as usize;
            let limit = i + len;
            let bytes = buffer[i..limit].to_vec();
            i = limit;
            let next1 = buffer.get(i);
            let next2 = buffer.get(i + 1);

            if next1 == Some(&b'\r') && next2 == Some(&b'\n') {
                i = i + 2;
                let str = std::str::from_utf8(&bytes).map_err(|_| ())?;
                return Ok((i, Self::BulkString(str.to_string())));
            } else {
                return Ok((i, Self::Bytes(bytes)));
            }
        }
    }

    fn map_plus(buffer: &[u8]) -> Result<(usize, RedisType), ()> {
        let mut i = 1;
        loop {
            let next_char = buffer[i] as char;
            if next_char == '\r' && buffer.get(i + 1) == Some(&b'\n') {
                i += 2;
                break;
            }
            i += 1;
        }

        let str = String::from_utf8(buffer[1..i - 2].to_vec()).map_err(|_| ())?;
        Ok((i, Self::SimpleString(str)))
    }

    fn map_minus(buffer: &[u8]) -> Result<(usize, RedisType), ()> {
        let mut i = 1;
        loop {
            let next_char = buffer[i] as char;
            if next_char == '\r' && buffer.get(i + 1) == Some(&b'\n') {
                i += 2;
                break;
            }
            i += 1;
        }

        let str = String::from_utf8(buffer[1..i - 2].to_vec()).map_err(|_| ())?;
        Ok((i, Self::SimpleError(str)))
    }

    fn map_colon(buffer: &[u8]) -> Result<(usize, RedisType), ()> {
        let mut i = 1;
        let mut negative = false;
        let mut sum: i64 = 0;
        loop {
            let next_char = buffer[i] as char;
            if next_char == '\r' && buffer.get(i + 1) == Some(&b'\n') {
                i = i + 2;
                break;
            }
            if next_char == '-' {
                negative = true;
            } else if next_char == '+' {
                negative = false;
            } else {
                let next_char = next_char.to_digit(10).ok_or(())? as i64;
                if negative {
                    sum = sum * 10 - next_char;
                } else {
                    sum = sum * 10 + next_char;
                }
            }
            i += 1;
        }

        Ok((i, Self::Integer(sum)))
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
            RedisType::NullArray => write!(f, "-1"),
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

#[cfg(test)]
mod test {
    use super::RedisType;

    #[test]
    fn test_from_buffer() {
        let buffer =
            b"+OK\r\n$-1\r\n$10\r\n09481nf8a-$5\r\nhello\r\n-Error message\r\n:100\r\n:-3214\r\n";
        let result = RedisType::from_buffer(buffer).unwrap();
        assert_eq!(
            result,
            vec![
                RedisType::SimpleString("OK".to_string()),
                RedisType::NullBulkString,
                RedisType::Bytes(b"09481nf8a-".to_vec()),
                RedisType::BulkString("hello".to_string()),
                RedisType::SimpleError("Error message".to_string()),
                RedisType::Integer(100),
                RedisType::Integer(-3214),
            ]
        );
    }

    #[test]
    fn test_from_buffer_2() {
        let buffer =
            b"+OK\r\n$-1\r\n$10\r\n09481nf8a-$5\r\nhello\r\n-Error message\r\n:100\r\n:-3214\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n-Error message\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*-1\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$5\r\nhello\r\n";
        let result = RedisType::from_buffer(buffer).unwrap();
        assert_eq!(
            result,
            vec![
                RedisType::SimpleString("OK".to_string()),
                RedisType::NullBulkString,
                RedisType::Bytes(b"09481nf8a-".to_vec()),
                RedisType::BulkString("hello".to_string()),
                RedisType::SimpleError("Error message".to_string()),
                RedisType::Integer(100),
                RedisType::Integer(-3214),
                RedisType::Array(vec![
                    RedisType::BulkString("SET".to_string()),
                    RedisType::BulkString("key".to_string()),
                    RedisType::BulkString("value".to_string())
                ]),
                RedisType::SimpleError("Error message".to_string()),
                RedisType::Array(vec![
                    RedisType::BulkString("SET".to_string()),
                    RedisType::BulkString("key".to_string()),
                    RedisType::BulkString("value".to_string())
                ]),
                RedisType::NullArray,
                RedisType::Array(vec![
                    RedisType::BulkString("SET".to_string()),
                    RedisType::BulkString("key".to_string()),
                    RedisType::BulkString("value".to_string())
                ]),
                RedisType::BulkString("hello".to_string())
            ]
        );
    }

    #[test]
    fn test_from_buffer_3() {
        let buffer =
            b"+OK\r\n$-1\r\n$10\r\n09481nf8a-$5\r\nhello\r\n-Error message\r\n:100\r\n:-3214\r\n*4\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n*-1\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let result = RedisType::from_buffer(buffer).unwrap();
        assert_eq!(
            result,
            vec![
                RedisType::SimpleString("OK".to_string()),
                RedisType::NullBulkString,
                RedisType::Bytes(b"09481nf8a-".to_vec()),
                RedisType::BulkString("hello".to_string()),
                RedisType::SimpleError("Error message".to_string()),
                RedisType::Integer(100),
                RedisType::Integer(-3214),
                RedisType::Array(vec![
                    RedisType::Array(vec![
                        RedisType::Integer(1),
                        RedisType::Integer(2),
                        RedisType::Integer(3)
                    ]),
                    RedisType::Array(vec![
                        RedisType::SimpleString("Hello".to_string()),
                        RedisType::SimpleError("World".to_string())
                    ]),
                    RedisType::NullArray,
                    RedisType::Array(vec![
                        RedisType::BulkString("SET".to_string()),
                        RedisType::BulkString("key".to_string()),
                        RedisType::BulkString("value".to_string())
                    ])
                ])
            ]
        );
    }

    #[test]
    fn test_from_buffer_4() {
        //Edge case request was being sent mangled to the server
        let buff = vec![
            43, 70, 85, 76, 76, 82, 69, 83, 89, 78, 67, 32, 103, 102, 54, 101, 107, 97, 48, 119,
            117, 55, 54, 100, 106, 111, 104, 52, 54, 110, 112, 49, 116, 51, 104, 118, 101, 105,
            121, 105, 120, 52, 110, 116, 122, 48, 108, 106, 57, 50, 99, 57, 32, 48, 13, 10, 36, 56,
            56, 13, 10, 82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45,
            118, 101, 114, 5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105,
            116, 115, 192, 64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117,
            115, 101, 100, 45, 109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98,
            97, 115, 101, 192, 0, 255, 240, 110, 59, 254, 192, 255, 90, 162, 42, 51, 13, 10, 36,
            51, 13, 10, 83, 69, 84, 13, 10, 36, 51, 13, 10, 102, 111, 111, 13, 10, 36, 51, 13, 10,
            49, 50, 51, 13, 10, 42, 51, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 51, 13, 10,
            98, 97, 114, 13, 10, 36, 51, 13, 10, 52, 53, 54, 13, 10, 42, 51, 13, 10, 36, 51, 13,
            10, 83, 69, 84, 13, 10, 36, 51, 13, 10, 98, 97, 122, 13, 10, 36, 51, 13, 10, 55, 56,
            57, 13, 10,
        ];

        RedisType::from_buffer(&buff).unwrap();
    }

    #[test]
    fn test_from_buffer_5() {
        let buffer = vec![
            43, 70, 85, 76, 76, 82, 69, 83, 89, 78, 67, 32, 116, 97, 117, 109, 48, 116, 54, 122,
            109, 112, 102, 102, 49, 57, 119, 111, 114, 104, 106, 57, 109, 51, 117, 102, 98, 109,
            107, 104, 49, 49, 107, 51, 115, 99, 108, 50, 104, 109, 111, 52, 32, 48, 13, 10,
        ];
        let result = RedisType::from_buffer(&buffer).unwrap();
        println!("{:?}", result);
        println!("    ");
        let buffer = vec![
            36, 56, 56, 13, 10, 82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105,
            115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45,
            98, 105, 116, 115, 192, 64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250,
            8, 117, 115, 101, 100, 45, 109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102,
            45, 98, 97, 115, 101, 192, 0, 255, 240, 110, 59, 254, 192, 255, 90, 162, 42, 51, 13,
            10, 36, 56, 13, 10, 82, 69, 80, 76, 67, 79, 78, 70, 13, 10, 36, 54, 13, 10, 71, 69, 84,
            65, 67, 75, 13, 10, 36, 49, 13, 10, 42, 13, 10,
        ];

        let result = RedisType::from_buffer(&buffer).unwrap();
        println!("{:?}", result);
    }
}
