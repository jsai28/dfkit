use std::path::{Path, PathBuf};
use datafusion::arrow::error::ArrowError;
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

    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),

    #[error("Error during execution: {0}")]
    CustomError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub fn file_type(
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
    let file_format = file_type(file_path);
    let file_name = file_path.to_str().ok_or(DfKitError::FileParse(FileParseError::InvalidExtension))?;
    match file_format? {
        FileFormat::Csv => ctx.register_csv(table_name, file_name, CsvReadOptions::default()).await?,
        FileFormat::Parquet => ctx.register_parquet(table_name, file_name, ParquetReadOptions::default()).await?,
        FileFormat::Json => ctx.register_json(table_name, file_name, NdJsonReadOptions::default()).await?,
        FileFormat::Avro => ctx.register_avro(table_name, file_name, AvroReadOptions::default()).await?,
    };

    Ok(ctx.table(table_name).await?)
}

pub fn parse_file_list(files: Option<String>, dir: Option<PathBuf>) -> Result<Vec<PathBuf>, DfKitError> {
    if let Some(file_str) = files {
        Ok(file_str.split(',')
            .map(|s| PathBuf::from(s.trim()))
            .collect())
    } else if let Some(dir_path) = dir {
        let mut file_list = vec![];
        for entry in std::fs::read_dir(dir_path)? {
            let path = entry?.path();
            if path.is_file() {
                file_list.push(path);
            }
        }
        Ok(file_list)
    } else {
        Err(DfKitError::CustomError("No files or directory provided".into()))
    }
}

