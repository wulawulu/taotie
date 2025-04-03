use crate::ReplContext;
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

use super::ReplCommands;

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
    let cmd = DescribeOpts::new(name).into();
    context.send(cmd);

    Ok(None)
}

impl From<DescribeOpts> for ReplCommands {
    fn from(opts: DescribeOpts) -> Self {
        ReplCommands::Describe(opts)
    }
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}
