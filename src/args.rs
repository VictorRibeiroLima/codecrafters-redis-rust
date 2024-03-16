use anyhow::Result;

use crate::redis::config::Config;

pub struct Args {
    pub port: u16,
    pub replica_of: Option<(String, u16)>,
    pub dir: Option<String>,
    pub db_file_name: Option<String>,
}

impl Args {
    pub fn parse() -> Result<Args> {
        let mut dir = None;
        let mut db_file_name = None;
        let mut replica_of = None;
        let mut port: u16 = 6379;
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => {
                    port = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Missing value for --port"))?
                        .parse()?;
                }
                "--replicaof" => {
                    let replica_of_s = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Missing value for --replicaof"))?;

                    let replica_of_u = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Missing value for --replicaof"))?
                        .parse()?;
                    replica_of = Some((replica_of_s, replica_of_u));
                }
                "--dir" => {
                    dir = Some(
                        args.next()
                            .ok_or_else(|| anyhow::anyhow!("Missing value for --dir"))?,
                    );
                }
                "--dbfilename" => {
                    db_file_name = Some(
                        args.next()
                            .ok_or_else(|| anyhow::anyhow!("Missing value for --dbfilename"))?,
                    );
                }
                _ => return Err(anyhow::anyhow!("Unknown argument: {}", arg)),
            }
        }
        Ok(Args {
            port,
            replica_of,
            dir,
            db_file_name,
        })
    }
}

impl Into<Config> for Args {
    fn into(self) -> Config {
        Config {
            port: self.port,
            replica_of: self.replica_of,
            dir: self.dir,
            db_file_name: self.db_file_name,
        }
    }
}
