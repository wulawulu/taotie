use clap::{ArgMatches, Parser};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use reedline_repl_rs::Result;

use crate::{Backend, CmdExecutor, ReplContext, ReplMsg};

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Parquet(String),
    Csv(FileOpts),
    Json(FileOpts),
}

#[derive(Debug, Clone)]
pub struct FileOpts {
    pub filename: String,
    pub extension: String,
    pub compression: FileCompressionType,
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
    let con_str = s.to_string();
    if con_str.starts_with("postgres://") {
        return Ok(DatasetConn::Postgres(con_str.to_string()));
    }
    if con_str.ends_with(".parquet") {
        return Ok(DatasetConn::Parquet(con_str.to_string()));
    }

    let parts = con_str.split('.').collect::<Vec<_>>();
    let len = parts.len();
    let mut parts = parts.into_iter().skip(1).take(len - 1);

    let r#type = parts.next();
    let compression = parts.next();

    match (compression, r#type) {
        (Some(compression), Some(r#type)) => {
            let compression = match compression {
                "gz" => FileCompressionType::GZIP,
                "bz2" => FileCompressionType::BZIP2,
                "xz" => FileCompressionType::XZ,
                "zstd" => FileCompressionType::ZSTD,
                v => return Err(format!("Invalid compression type: {}", v)),
            };
            let opts = FileOpts {
                filename: s.to_string(),
                extension: r#type.to_string(),
                compression,
            };
            match r#type {
                "csv" => Ok(DatasetConn::Csv(opts)),
                "json" | "ndjson" | "jsonl" => Ok(DatasetConn::Json(opts)),
                v => Err(format!("Invliad file extension: {}", v)),
            }
        }
        (None, Some(r#type)) => {
            let opts = FileOpts {
                filename: s.to_string(),
                extension: r#type.to_string(),
                compression: FileCompressionType::UNCOMPRESSED,
            };
            match r#type {
                "csv" => Ok(DatasetConn::Csv(opts)),
                "json" | "ndjson" | "jsonl" => Ok(DatasetConn::Json(opts)),
                _ => Err(format!("Unsupported dataset connection: {}", con_str)),
            }
        }
        _ => Err(format!("Unsupported dataset connection: {}", con_str)),
    }
}
