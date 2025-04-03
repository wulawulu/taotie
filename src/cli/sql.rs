use super::ReplCommands;
use crate::ReplContext;
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
    let cmd = SqlOpts::new(sql).into();
    context.send(cmd);

    Ok(None)
}

impl From<SqlOpts> for ReplCommands {
    fn from(opts: SqlOpts) -> Self {
        ReplCommands::Sql(opts)
    }
}

impl SqlOpts {
    pub fn new(sql: String) -> Self {
        Self { query: sql }
    }
}
