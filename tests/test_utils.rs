use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use datafusion::error::DataFusionError;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use tempfile::{tempdir, NamedTempFile};
use dfkit::utils::{download_to_tempfile, file_type, parse_file_list, register_table, write_output, DfKitError, FileFormat, FileParseError};

#[tokio::test]
async fn test_register_table_remote_csv() {
    let ctx = SessionContext::new();
    let url = "https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv";

    let df = register_table(&ctx, "remote_csv", Path::new(url)).await.unwrap();
    let results = df.collect().await.unwrap();

    assert!(!results.is_empty());
}

#[test]
fn test_file_type_supported() {
    assert_eq!(file_type(Path::new("file.csv")).unwrap(), FileFormat::Csv);
    assert_eq!(file_type(Path::new("file.parquet")).unwrap(), FileFormat::Parquet);
    assert_eq!(file_type(Path::new("file.json")).unwrap(), FileFormat::Json);
    assert_eq!(file_type(Path::new("file.avro")).unwrap(), FileFormat::Avro);
}

#[test]
fn test_file_type_unsupported() {
    let err = file_type(Path::new("file.txt")).unwrap_err();
    assert!(matches!(err, FileParseError::UnsupportedFileFormat));
}

#[test]
fn test_file_type_invalid_extension() {
    let err = file_type(Path::new("file")).unwrap_err();
    assert!(matches!(err, FileParseError::InvalidExtension));
}

#[test]
fn test_parse_file_list_from_files() {
    let result = parse_file_list(Some("file1.csv, file2.csv".into()), None).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], PathBuf::from("file1.csv"));
    assert_eq!(result[1], PathBuf::from("file2.csv"));
}

#[test]
fn test_parse_file_list_from_dir() {
    let dir = tempdir().unwrap();
    let file1 = dir.path().join("a.csv");
    let file2 = dir.path().join("b.csv");
    File::create(&file1).unwrap();
    File::create(&file2).unwrap();

    let result = parse_file_list(None, Some(dir.path().to_path_buf())).unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn test_parse_file_list_none() {
    let err = parse_file_list(None, None).unwrap_err();
    assert!(matches!(err, DfKitError::CustomError(_)));
}

#[tokio::test]
async fn test_download_to_tempfile() {
    let url = "https://raw.githubusercontent.com/apache/arrow-datafusion/main/datafusion/examples/data/simple.csv";
    let (_tempfile, path) = download_to_tempfile(url).await.unwrap();
    assert!(path.exists());
    assert_eq!(path.extension().unwrap(), "csv");
}

#[tokio::test]
async fn test_write_output_and_read_back() {
    let ctx = SessionContext::new();
    let csv_data = "id,name\n1,Alice\n2,Bob\n";

    // Create temp dir and write input CSV file
    let tmp_dir = tempdir().unwrap();
    let input_path = tmp_dir.path().join("input.csv");
    std::fs::write(&input_path, csv_data).unwrap();

    // Read the CSV into a DataFrame
    let df = ctx.read_csv(input_path.to_str().unwrap(), CsvReadOptions::default()).await.unwrap();

    // Define output path with .csv extension
    let out_path = tmp_dir.path().join("output.csv");

    // Write to CSV (should succeed)
    write_output(df.clone(), &out_path, &FileFormat::Csv).await.unwrap();
    assert!(out_path.exists());

    // Try writing to Avro (should fail with NotImplemented)
    let err = write_output(df, &out_path, &FileFormat::Avro).await.unwrap_err();
    assert!(matches!(err, DfKitError::DataFusion(DataFusionError::NotImplemented(_))));
}

#[tokio::test]
async fn test_register_table_csv() {
    let ctx = SessionContext::new();

    // Create a temporary file with .csv extension
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");

    // Write CSV content to it
    std::fs::write(&file_path, "id,name\n1,Alice\n2,Bob").unwrap();

    // Register table and verify contents
    let df = register_table(&ctx, "test_table", &file_path).await.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
}