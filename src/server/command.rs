use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo,
}

impl FromStr for Command {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PING" => Ok(Command::Ping),
            "ECHO" => Ok(Command::Echo),
            _ => Err(()),
        }
    }
}

pub fn handle_command(command: Command, args: Vec<&str>) -> String {
    match command {
        Command::Ping => "+PONG\r\n".to_string(),
        Command::Echo => format!("+{}\r\n", args.join(" ")),
    }
}

fn handle_ping() -> String {
    "+PONG\r\n".to_string()
}

fn handle_echo(args: Vec<&str>) -> String {
    let first_arg = args.get(0).unwrap_or(&"");
    format!("+{}\r\n", first_arg)
}
