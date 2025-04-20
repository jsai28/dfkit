use datafusion::arrow::error::ArrowError;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use reqwest::Client;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use thiserror::Error;

#[derive(Debug, PartialEq, Eq)]
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

    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
}

pub fn file_type(file_path: &Path) -> Result<FileFormat, FileParseError> {
    match Path::new(file_path)
        .extension()
        .and_then(|ext| ext.to_str())
    {
        Some("csv") => Ok(FileFormat::Csv),
        Some("parquet") => Ok(FileFormat::Parquet),
        Some("json") => Ok(FileFormat::Json),
        Some("avro") => Ok(FileFormat::Avro),
        Some(_) => Err(FileParseError::UnsupportedFileFormat),
        None => Err(FileParseError::InvalidExtension),
    }
}

pub async fn register_table(
    ctx: &SessionContext,
    table_name: &str,
    file_path: &Path,
) -> Result<DataFrame, DfKitError> {
    let path_str = file_path
        .to_str()
        .ok_or(DfKitError::FileParse(FileParseError::InvalidExtension))?;
    let is_url = path_str.starts_with("http://") || path_str.starts_with("https://");

    let actual_path = if is_url {
        let (_tmpfile, local_path) = download_to_tempfile(path_str).await?;
        local_path
    } else {
        file_path.to_path_buf()
    };

    let file_format = file_type(&actual_path)?;
    let file_name = actual_path
        .to_str()
        .ok_or(DfKitError::FileParse(FileParseError::InvalidExtension))?;
    match file_format {
        FileFormat::Csv => {
            ctx.register_csv(table_name, file_name, CsvReadOptions::default())
                .await?
        }
        FileFormat::Parquet => {
            ctx.register_parquet(table_name, file_name, ParquetReadOptions::default())
                .await?
        }
        FileFormat::Json => {
            ctx.register_json(table_name, file_name, NdJsonReadOptions::default())
                .await?
        }
        FileFormat::Avro => {
            ctx.register_avro(table_name, file_name, AvroReadOptions::default())
                .await?
        }
    };

    Ok(ctx.table(table_name).await?)
}

pub fn parse_file_list(
    files: Option<String>,
    dir: Option<PathBuf>,
) -> Result<Vec<PathBuf>, DfKitError> {
    if let Some(file_str) = files {
        Ok(file_str
            .split(',')
            .map(|s| PathBuf::from(s.trim()))
            .collect())
    } else if let Some(dir_path) = dir {
        let mut file_list = vec![];
        for entry in std::fs::read_dir(dir_path)? {
            let path = entry?.path();
            if path.is_file() {
                if file_type(&path).is_ok() {
                    file_list.push(path);
                } else {
                    println!("{:?} is not a compatible file, skipping...", path);
                }
            }
        }
        Ok(file_list)
    } else {
        Err(DfKitError::CustomError(
            "No files or directory provided".into(),
        ))
    }
}

pub async fn write_output(
    df: DataFrame,
    out_path: &Path,
    format: &FileFormat,
) -> Result<(), DfKitError> {
    match format {
        FileFormat::Csv => {
            df.write_csv(
                out_path.to_str().unwrap(),
                DataFrameWriteOptions::default(),
                None,
            )
            .await?
        }
        FileFormat::Parquet => {
            df.write_parquet(
                out_path.to_str().unwrap(),
                DataFrameWriteOptions::default(),
                None,
            )
            .await?
        }
        FileFormat::Json => {
            df.write_json(
                out_path.to_str().unwrap(),
                DataFrameWriteOptions::default(),
                None,
            )
            .await?
        }
        FileFormat::Avro => {
            return Err(DfKitError::DataFusion(DataFusionError::NotImplemented(
                "Avro write not supported".into(),
            )));
        }
    };
    Ok(())
}

pub async fn download_to_tempfile(url: &str) -> Result<(NamedTempFile, PathBuf), DfKitError> {
    let response = Client::new().get(url).send().await?.bytes().await?;

    // Try to extract the file extension from the URL
    let ext = url
        .split('.')
        .last()
        .and_then(|e| {
            let e = e.split('?').next().unwrap_or(e); // strip query string
            match e {
                "csv" | "json" | "parquet" | "avro" => Some(e),
                _ => None,
            }
        })
        .ok_or(FileParseError::InvalidExtension)?;

    // Create temp file with extension
    let tempfile = NamedTempFile::new()?;
    let mut path_with_ext = tempfile.path().to_path_buf();
    path_with_ext.set_extension(ext);

    std::fs::copy(tempfile.path(), &path_with_ext)?;
    std::fs::write(&path_with_ext, &response)?;

    Ok((tempfile, path_with_ext))
}
