use std::ops::Deref;

use crate::{Backend, ConnectOpts, DatasetConn, ReplDisplay};
use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{SessionConfig, SessionContext};

pub struct DatafusionBackend(SessionContext);

impl DatafusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;

        Self(SessionContext::new_with_config(config))
    }
}

impl Backend for DatafusionBackend {
    type DataFrame = datafusion::dataframe::DataFrame;
    async fn connect(&mut self, opts: &ConnectOpts) -> Result<()> {
        match &opts.conn {
            DatasetConn::Parquet(path) => {
                self.register_parquet(&opts.name, path, Default::default())
                    .await?;
            }
            DatasetConn::Postgres(_) => todo!(),
            DatasetConn::Csv(path) => {
                self.register_csv(&opts.name, path, Default::default())
                    .await?;
            }
            DatasetConn::Json(path) => {
                self.register_json(&opts.name, path, Default::default())
                    .await?;
            }
        }
        Ok(())
    }

    async fn describe(&self, name: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self.sql(&format!("SELECT * FROM {}", name)).await?;
        let schema = df.describe().await?;
        Ok(schema)
    }

    async fn head(&self, name: &str, size: usize) -> anyhow::Result<Self::DataFrame> {
        let df = self
            .sql(&format!("SELECT * FROM {} LIMIT {}", name, size))
            .await?;
        Ok(df)
    }

    async fn list(&self) -> anyhow::Result<Self::DataFrame> {
        let df = self.sql("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'").await?;
        Ok(df)
    }

    async fn schema(&self, name: &str) -> Result<Self::DataFrame> {
        let df = self.sql(&format!("DESCRIBE {}", name)).await?;
        Ok(df)
    }

    async fn sql(&self, sql: &str) -> anyhow::Result<Self::DataFrame> {
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

impl ReplDisplay for datafusion::dataframe::DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let results = self.collect().await?;
        let data = pretty_format_batches(&results)?;
        Ok(data.to_string())
    }
}
