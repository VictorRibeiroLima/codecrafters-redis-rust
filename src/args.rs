use anyhow::Result;

pub struct Args {
    pub port: u16,
}

impl Args {
    pub fn parse() -> Result<Args> {
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
                _ => return Err(anyhow::anyhow!("Unknown argument: {}", arg)),
            }
        }
        Ok(Args { port })
    }
}
