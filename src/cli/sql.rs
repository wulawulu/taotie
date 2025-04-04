use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;
#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(short, long, help = "the sql to run")]
    query: String,
}

pub fn sql(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let sql = args
        .get_one::<String>("query")
        .expect("expect query")
        .to_string();
    let (msg, rx) = ReplMsg::new(SqlOpts::new(sql));

    Ok(context.send(msg, rx))
}

impl CmdExecutor for SqlOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.sql(&self.query).await?;
        df.display().await
    }
}

impl SqlOpts {
    pub fn new(sql: String) -> Self {
        Self { query: sql }
    }
}
