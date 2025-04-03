use crate::ReplContext;
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

use super::ReplCommands;

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(short, long, help = "the name of the dataset")]
    name: String,
    #[arg(short, long, help = "the number of rows to display")]
    n: Option<usize>,
}

pub fn head(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();
    let n = args.get_one::<usize>("n").copied();

    let cmd = HeadOpts::new(name, n).into();
    context.send(cmd);

    Ok(None)
}

impl From<HeadOpts> for ReplCommands {
    fn from(opts: HeadOpts) -> Self {
        ReplCommands::Head(opts)
    }
}

impl HeadOpts {
    pub fn new(name: String, n: Option<usize>) -> Self {
        Self { name, n }
    }
}
