pub mod describe;

use std::ops::Deref;

use crate::{Backend, ConnectOpts, DatasetConn, ReplDisplay};
use anyhow::Result;
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion::prelude::{
    CsvReadOptions, DataFrame, NdJsonReadOptions, SessionConfig, SessionContext,
};
use describe::DataFrameDescriber;

pub struct DatafusionBackend(SessionContext);

impl DatafusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;

        Self(SessionContext::new_with_config(config))
    }
}

impl Backend for DatafusionBackend {
    async fn connect(&mut self, opts: &ConnectOpts) -> Result<()> {
        match &opts.conn {
            DatasetConn::Parquet(path) => {
                self.register_parquet(&opts.name, path, Default::default())
                    .await?;
            }
            DatasetConn::Postgres(_) => todo!(),
            DatasetConn::Csv(file_opts) => {
                let options = CsvReadOptions {
                    file_extension: &file_opts.extension,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };
                self.register_csv(&opts.name, &file_opts.filename, options)
                    .await?;
            }
            DatasetConn::Json(file_opts) => {
                let options = NdJsonReadOptions {
                    file_extension: &file_opts.extension,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };
                self.register_json(&opts.name, &file_opts.filename, options)
                    .await?;
            }
        }
        Ok(())
    }

    async fn describe(&self, name: &str) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(&format!("SELECT * FROM {}", name)).await?;
        let describe_df = DataFrameDescriber::try_new(df)?;
        let describe_df = describe_df.describe().await?;
        Ok(describe_df)
    }

    async fn head(&self, name: &str, size: usize) -> anyhow::Result<impl ReplDisplay> {
        let df = self
            .0
            .sql(&format!("SELECT * FROM {} LIMIT {}", name, size))
            .await?;
        Ok(df)
    }

    async fn list(&self) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'").await?;
        Ok(df)
    }

    async fn schema(&self, name: &str) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(&format!("DESCRIBE {}", name)).await?;
        Ok(df)
    }

    async fn sql(&self, sql: &str) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(sql).await?;
        Ok(df)
    }
}

impl Default for DatafusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for DatafusionBackend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReplDisplay for DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let results = self.collect().await?;
        let data = pretty_format_batches(&results)?;
        Ok(data.to_string())
    }
}

impl ReplDisplay for RecordBatch {
    async fn display(self) -> anyhow::Result<String> {
        let data = pretty_format_batches(&[self])?;
        Ok(data.to_string())
    }
}
