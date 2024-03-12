pub struct BulkString {
    pub lines: Vec<String>,
}

impl BulkString {
    pub fn new() -> Self {
        Self { lines: Vec::new() }
    }

    pub fn push(&mut self, line: String) {
        self.lines.push(line);
    }

    pub fn encode(&self) -> String {
        let mut bulk_string = String::from("*");
        bulk_string.push_str(&self.lines.len().to_string());
        bulk_string.push_str("\r\n");
        for line in &self.lines {
            bulk_string.push_str("$");
            bulk_string.push_str(&line.len().to_string());
            bulk_string.push_str("\r\n");
            bulk_string.push_str(&line);
            bulk_string.push_str("\r\n");
        }
        bulk_string
    }
}
