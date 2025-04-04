use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(short, long, help = "the name of the dataset")]
    name: String,
    #[arg(short, long, help = "the number of rows to display")]
    size: Option<usize>,
}

pub fn head(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();
    let size = args.get_one::<usize>("size").copied();

    let (msg, rx) = ReplMsg::new(HeadOpts::new(name, size));

    Ok(context.send(msg, rx))
}

impl CmdExecutor for HeadOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.head(&self.name, self.size.unwrap_or(10)).await?;
        df.display().await
    }
}

impl HeadOpts {
    pub fn new(name: String, size: Option<usize>) -> Self {
        Self { name, size }
    }
}
