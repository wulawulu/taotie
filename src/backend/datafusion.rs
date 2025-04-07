use std::{ops::Deref, sync::Arc};

use crate::{Backend, ConnectOpts, DatasetConn, ReplDisplay};
use anyhow::Result;
use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::{cast, concat},
    datatypes::{DataType, Field, Schema, SchemaRef},
    util::pretty::pretty_format_batches,
};
use datafusion::{
    error::DataFusionError,
    functions_aggregate::expr_fn::{avg, count, max, median, min, stddev, sum},
    prelude::{
        CsvReadOptions, DataFrame, NdJsonReadOptions, SessionConfig, SessionContext, case, col,
        is_null, lit,
    },
};

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
        let describe_df = DescribeDataFusion::new(df);
        let describe_df = describe_df.to_describe_record_batch().await?;
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

struct DescribeDataFusion {
    df: DataFrame,
    schema: SchemaRef,
    functions: Vec<&'static str>,
}

impl DescribeDataFusion {
    fn new(df: DataFrame) -> Self {
        //the functions now supported
        let supported_describe_functions =
            vec!["count", "null_count", "mean", "std", "min", "max", "median"];

        let original_schema_fields = df.schema().fields().iter();

        let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];

        describe_schemas.extend(original_schema_fields.clone().map(|field| {
            if field.data_type().is_numeric() {
                Field::new(field.name(), DataType::Float64, true)
            } else {
                Field::new(field.name(), DataType::Utf8, true)
            }
        }));
        let schema = Arc::new(Schema::new(describe_schemas));
        Self {
            df,
            schema,
            functions: supported_describe_functions,
        }
    }

    async fn to_describe_record_batch(&self) -> Result<RecordBatch> {
        let original_schema_fields = self.df.schema().fields().iter();

        //collect recordBatch
        let describe_record_batch = vec![
            self.count(),
            self.null_count(),
            self.mean(),
            self.stddev(),
            self.min(),
            self.max(),
            self.median(),
        ];

        // first column with function names
        let mut function_colume: Vec<ArrayRef> =
            vec![Arc::new(StringArray::from(self.functions.clone()))];
        for field in original_schema_fields {
            let mut array_data = vec![];
            for result in describe_record_batch.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batches = df.clone().collect().await;
                        match batches {
                            Ok(batches)
                                if batches.len() == 1
                                    && batches[0].column_by_name(field.name()).is_some() =>
                            {
                                let column = batches[0].column_by_name(field.name()).unwrap();

                                if column.data_type().is_null() {
                                    Arc::new(StringArray::from(vec!["null"]))
                                } else if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    //Handling error when only boolean/binary column, and in other cases
                    Err(err)
                        if err.to_string().contains(
                            "Error during planning: \
                                            Aggregate requires at least one grouping \
                                            or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(other_err) => {
                        panic!("{other_err}")
                    }
                };
                array_data.push(array_ref);
            }
            function_colume.push(concat(
                array_data
                    .iter()
                    .map(|af| af.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?);
        }

        let ret = RecordBatch::try_new(self.schema.clone(), function_colume)?;
        Ok(ret)
    }

    fn count(&self) -> Result<DataFrame, DataFusionError> {
        let original_schema_fields = self.df.schema().fields().iter();

        self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| count(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )
    }

    fn null_count(&self) -> Result<DataFrame, DataFusionError> {
        let original_schema_fields = self.df.schema().fields().iter();

        self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| {
                    sum(case(is_null(col(f.name())))
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap())
                    .alias(f.name())
                })
                .collect::<Vec<_>>(),
        )
    }

    fn mean(&self) -> Result<DataFrame, DataFusionError> {
        let original_schema_fields = self.df.schema().fields().iter();

        self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| avg(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )
    }

    fn stddev(&self) -> Result<DataFrame, DataFusionError> {
        let original_schema_fields = self.df.schema().fields().iter();

        self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| stddev(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )
    }

    fn min(&self) -> Result<DataFrame, DataFusionError> {
        let original_schema_fields = self.df.schema().fields().iter();

        self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| min(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )
    }

    fn max(&self) -> Result<DataFrame, DataFusionError> {
        let original_schema_fields = self.df.schema().fields().iter();

        self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| max(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )
    }

    fn median(&self) -> Result<DataFrame, DataFusionError> {
        let original_schema_fields = self.df.schema().fields().iter();

        self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| median(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )
    }
}
