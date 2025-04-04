use clap::Parser;
pub use connect::{ConnectOpts, DatasetConn};
pub use describe::DescribeOpts;
use enum_dispatch::enum_dispatch;
pub use head::HeadOpts;
pub use list::ListOpts;
pub use schema::SchemaOpts;
pub use sql::SqlOpts;

mod connect;
mod describe;
mod head;
mod list;
mod schema;
mod sql;

pub use connect::connect;
pub use describe::describe;
pub use head::head;
pub use list::list;
pub use schema::schema;
pub use sql::sql;

#[derive(Parser, Debug)]
#[command(
    name = "cli",
    version = "0.1.0",
    author = "wu",
    about = "explore dataset"
)]
pub struct ReplCommand {
    #[command(subcommand)]
    pub command: ReplCommands,
}

#[derive(Debug, Parser)]
#[enum_dispatch]
pub enum ReplCommands {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to Taotie"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List(ListOpts),
    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),
    #[command(about = "Show first few rows of a dataset")]
    Head(HeadOpts),
    #[command(about = "Query a dataset using given SQL")]
    Sql(SqlOpts),
    #[command(about = "Show the schema of a dataset")]
    Schema(SchemaOpts),
}
