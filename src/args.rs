use anyhow::Result;

pub struct Args {
    pub port: u16,
    pub replica_of: Option<u16>,
}

impl Args {
    pub fn parse() -> Result<Args> {
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
                    let replica_of_o = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("Missing value for --replicaof"))?
                        .parse()?;
                    replica_of = Some(replica_of_o);
                }
                _ => return Err(anyhow::anyhow!("Unknown argument: {}", arg)),
            }
        }
        Ok(Args { port, replica_of })
    }
}
