use std::path::Path;
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use crate::utils::{file_type, register_table, DfKitError, FileFormat};

pub async fn convert(ctx: &SessionContext, filename: &Path, output_filename: &Path) -> Result<(), DfKitError> {
    let df = register_table(ctx, "t", &filename).await?;
    let output_file_type = file_type(&output_filename)?;

    match output_file_type {
        FileFormat::Csv => df.write_csv(output_filename.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
        FileFormat::Parquet => df.write_parquet(output_filename.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
        FileFormat::Json => df.write_json(output_filename.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
        FileFormat::Avro => {
            return Err(DfKitError::DataFusion(DataFusionError::NotImplemented("Avro write support not implemented".to_string())));
        }
    };
    Ok(())
}