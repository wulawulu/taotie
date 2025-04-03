use reedline_repl_rs::{Repl, Result};
use taotie::{ReplCommand, ReplContext, callbacks_map};

const HISTORY_SIZE: usize = 1024;

fn main() -> Result<()> {
    let callbacks = callbacks_map();

    let ctx = ReplContext::new();

    let history_file = dirs::home_dir()
        .expect("expect home dir")
        .join(".taotie_history");

    let mut repl = Repl::new(ctx)
        .with_history(history_file, HISTORY_SIZE)
        .with_banner("Welcome to Taotie, your dataset exploration REPL!")
        .with_derived::<ReplCommand>(callbacks);

    repl.run()
}
