use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

use super::ReplCommands;

#[derive(Debug, Parser)]
pub struct ListOpts;

pub fn list(_args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let (msg, rx) = ReplMsg::new(ReplCommands::List(ListOpts));

    Ok(context.send(msg, rx))
}

impl CmdExecutor for ListOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.list().await?;
        df.display().await
    }
}
