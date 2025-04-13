use std::path::Path;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Debug)]
pub enum FileFormat {
    Csv,
    Parquet,
    Json,
    Avro,
}

#[derive(Error, Debug)]
pub enum FileParseError {
    #[error("unsupported file format")]
    UnsupportedFileFormat,
    #[error("invalid file extension")]
    InvalidExtension,
}

#[derive(Error, Debug)]
pub enum DfKitError {
    #[error("File parsing error: {0}")]
    FileParse(#[from] FileParseError),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("Unknown error")]
    Unknown,
}

pub fn parse_file(
    file_path: &Path,
) -> Result<FileFormat,FileParseError> {
    match Path::new(file_path).extension().and_then(|ext| ext.to_str()) {
      Some("csv") => Ok(FileFormat::Csv),
      Some("parquet") => Ok(FileFormat::Parquet),
      Some("json") => Ok(FileFormat::Json),
      Some("avro") => Ok(FileFormat::Avro),
      Some(_) => Err(FileParseError::UnsupportedFileFormat),
      None => Err(FileParseError::InvalidExtension),
  }
}

pub async fn register_table(ctx: &SessionContext, table_name: &str, file_path: &Path) -> Result<DataFrame, DfKitError> {
    let file_format = parse_file(file_path);
    let file_name = file_path.to_str().ok_or(DfKitError::FileParse(FileParseError::InvalidExtension))?;
    match file_format? {
        FileFormat::Csv => ctx.register_csv(table_name, file_name, CsvReadOptions::default()).await?,
        FileFormat::Parquet => ctx.register_parquet(table_name, file_name, ParquetReadOptions::default()).await?,
        FileFormat::Json => ctx.register_json(table_name, file_name, NdJsonReadOptions::default()).await?,
        FileFormat::Avro => ctx.register_avro(table_name, file_name, AvroReadOptions::default()).await?,
    };

    Ok(ctx.table(table_name).await?)
}
