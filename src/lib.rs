mod backend;
mod cli;

use std::{ops::Deref, thread};

use backend::DatafusionBackend;
pub use cli::*;
use crossbeam::channel::Sender;
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::CallBackMap;

use anyhow::Result;
use tokio::runtime::Runtime;

trait Backend {
    type DataFrame: ReplDisplay;
    async fn connect(&mut self, opts: &ConnectOpts) -> Result<()>;
    async fn describe(&self, name: &str) -> Result<Self::DataFrame>;
    async fn head(&self, name: &str, size: usize) -> Result<Self::DataFrame>;
    async fn list(&self) -> Result<Self::DataFrame>;
    async fn schema(&self, name: &str) -> Result<Self::DataFrame>;
    async fn sql(&self, sql: &str) -> Result<Self::DataFrame>;
}

#[enum_dispatch(ReplCommands)]
trait CmdExecutor {
    async fn execute<T: Backend>(&self, backend: &mut T) -> Result<String>;
}

trait ReplDisplay {
    async fn display(self) -> anyhow::Result<String>;
}

pub struct ReplContext {
    sender: Sender<ReplMsg>,
}

pub struct ReplMsg {
    pub command: ReplCommands,
    pub tx: oneshot::Sender<String>,
}

pub type ReplCallBacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn callbacks_map() -> ReplCallBacks {
    let mut callbacks = ReplCallBacks::new();
    callbacks.insert("connect".to_string(), connect);
    callbacks.insert("describe".to_string(), describe);
    callbacks.insert("head".to_string(), head);
    callbacks.insert("list".to_string(), list);
    callbacks.insert("sql".to_string(), sql);
    callbacks.insert("schema".to_string(), schema);
    callbacks
}

impl ReplContext {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam::channel::unbounded::<ReplMsg>();
        let mut backend = DatafusionBackend::new();
        let rt = Runtime::new().expect("Failed to create runtime");
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(msg) = receiver.recv() {
                    if let Err(e) = rt.block_on(async {
                        let result = msg.command.execute(&mut backend).await?;
                        msg.tx.send(result)?;
                        Ok::<_, anyhow::Error>(())
                    }) {
                        eprintln!("Failed to process command: {}", e);
                        std::process::exit(1);
                    }
                }
            })
            .unwrap();
        Self { sender }
    }

    pub fn send(&self, command: ReplMsg, rx: oneshot::Receiver<String>) -> Option<String> {
        if let Err(e) = self.sender.send(command) {
            eprintln!("Failed to send command: {} to backend", e);
            std::process::exit(1);
        }
        match rx.recv() {
            Ok(data) => Some(data),
            Err(e) => {
                eprintln!("Failed to receive command process result: {}", e);
                std::process::exit(1);
            }
        }
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for ReplContext {
    type Target = Sender<ReplMsg>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl ReplMsg {
    pub fn new(command: impl Into<ReplCommands>) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        let msg = Self {
            command: command.into(),
            tx,
        };
        (msg, rx)
    }
}
