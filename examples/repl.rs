use std::collections::HashMap;

use clap::{ArgMatches, Parser, Subcommand};
use reedline_repl_rs::{CallBackMap, Repl, Result};

#[derive(Parser, Debug)]
pub struct MyApp {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Say {
        #[command(subcommand)]
        command: SayCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum SayCommand {
    Hello {
        #[arg(required = true)]
        who: String,
        uppercase: String,
    },
    Goodbye {
        #[arg(long,action=clap::ArgAction::SetTrue)]
        spanish: bool,
    },
}

fn say<T>(args: ArgMatches, _context: &mut T) -> Result<Option<String>> {
    match args.subcommand() {
        Some(("hello", sub_matches)) => Ok(Some(format!(
            "Hello, {}!",
            sub_matches.get_one::<String>("who").unwrap()
        ))),
        Some(("goodbye", sub_matches)) => Ok(Some(if sub_matches.get_flag("spanish") {
            "Adios".to_string()
        } else {
            "Goodbye".to_string()
        })),
        _ => panic!("Unknown subcommand : {:?}", args.subcommand_name()),
    }
}

fn main() -> Result<()> {
    let mut callbacks: CallBackMap<(), reedline_repl_rs::Error> = HashMap::new();

    callbacks.insert("say".to_string(), say);

    let mut repl = Repl::new(())
        .with_banner("Welcome to MyApp")
        .with_derived::<MyApp>(callbacks);

    repl.run()
}
