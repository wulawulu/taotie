use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

use crate::{Backend, CmdExecutor, ReplContext, ReplMsg};

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Parquet(String),
    Csv(String),
    Json(String),
}

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    #[arg(value_parser = parse_dataset_conn,help="Connection string to the dataset, could be postgres or local file (support parquet, csv, json)")]
    pub conn: DatasetConn,
    #[arg(short, long, help = "If database, the name of the table")]
    pub table: Option<String>,
    #[arg(short, long, help = "the name of the dataset")]
    pub name: String,
}

pub fn connect(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let conn = args
        .get_one::<DatasetConn>("conn")
        .expect("expect conn_str")
        .to_owned();
    let table = args.get_one::<String>("table").map(|s| s.to_string());
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let (msg, rx) = ReplMsg::new(ConnectOpts::new(conn, table, name));

    Ok(context.send(msg, rx))
}

impl ConnectOpts {
    pub fn new(conn: DatasetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}

impl CmdExecutor for ConnectOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(self).await?;
        Ok(format!("Connected to dataset {}", self.name))
    }
}

fn parse_dataset_conn(s: &str) -> std::result::Result<DatasetConn, String> {
    if s.starts_with("postgres://") {
        Ok(DatasetConn::Postgres(s.to_string()))
    } else if s.ends_with(".parquet") {
        Ok(DatasetConn::Parquet(s.to_string()))
    } else if s.ends_with(".csv") {
        Ok(DatasetConn::Csv(s.to_string()))
    } else if s.ends_with(".json") {
        Ok(DatasetConn::Json(s.to_string()))
    } else {
        Err(format!("Unsupported dataset connection: {}", s))
    }
}
