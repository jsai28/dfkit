use std::path::{Path, PathBuf};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use datafusion::error::DataFusionError;
use crate::utils::{file_type, register_table, DfKitError, FileFormat};

pub async fn query(ctx: &SessionContext, filename: &Path, sql: Option<String>, output: Option<PathBuf>) -> Result<(), DfKitError> {
    let file_type = file_type(&filename)?;
    let _ = register_table(&ctx, "t", &filename).await?;
    let df_sql = ctx.sql(&*sql.unwrap()).await?;

    if let Some(path) = output {
        match file_type {
            FileFormat::Csv => df_sql.write_csv(path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
            FileFormat::Parquet => df_sql.write_parquet(path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
            FileFormat::Json => df_sql.write_json(path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
            FileFormat::Avro => {
                return Err(DfKitError::DataFusion(DataFusionError::NotImplemented("Avro write support not implemented".to_string())));
            }
        };

        println!("File written to: {}, successfully.", path.display());
    } else {
        df_sql.show().await?;
    }

    Ok(())
}