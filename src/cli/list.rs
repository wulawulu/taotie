use crate::ReplContext;
use clap::ArgMatches;
use reedline_repl_rs::Result;

use super::ReplCommands;

pub fn list(_args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    context.send(ReplCommands::List);

    Ok(None)
}
