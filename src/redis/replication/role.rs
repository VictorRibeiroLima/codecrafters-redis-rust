use std::fmt::Display;

#[derive(Debug, PartialEq, Default)]
#[allow(dead_code)]
pub enum Role {
    #[default]
    Master,
    Slave,
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Master => write!(f, "master"),
            Role::Slave => write!(f, "slave"),
        }
    }
}
