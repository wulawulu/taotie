use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(short, long, help = "the name of the dataset")]
    name: String,
}

pub fn describe(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();
    let (msg, rx) = ReplMsg::new(DescribeOpts::new(name));

    Ok(context.send(msg, rx))
}

impl CmdExecutor for DescribeOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.describe(&self.name).await?;
        df.display().await
    }
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}
