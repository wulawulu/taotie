mod cli;

use std::{ops::Deref, thread};

pub use cli::*;
use crossbeam::channel::Sender;
use reedline_repl_rs::CallBackMap;

pub struct ReplContext {
    sender: Sender<ReplCommands>,
}

pub type ReplCallBacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn callbacks_map() -> ReplCallBacks {
    let mut callbacks = ReplCallBacks::new();
    callbacks.insert("connect".to_string(), connect);
    callbacks.insert("describe".to_string(), describe);
    callbacks.insert("head".to_string(), head);
    callbacks.insert("list".to_string(), list);
    callbacks.insert("sql".to_string(), sql);
    callbacks
}

impl ReplContext {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam::channel::unbounded();
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    match command {
                        ReplCommands::Connect(opts) => println!("backend connect {:?}", opts),
                        ReplCommands::Describe(describe_opts) => {
                            println!("backend describe {:?}", describe_opts)
                        }
                        ReplCommands::List => println!("backend list"),
                        ReplCommands::Head(head_opts) => println!("backend head {:?}", head_opts),
                        ReplCommands::Sql(sql_opts) => println!("backend sql {:?}", sql_opts),
                    }
                }
            })
            .unwrap();
        Self { sender }
    }

    pub fn send(&self, command: ReplCommands) {
        if let Err(e) = self.sender.send(command) {
            eprintln!("Failed to send command: {} to backend", e);
            std::process::exit(1);
        }
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for ReplContext {
    type Target = Sender<ReplCommands>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}
