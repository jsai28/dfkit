use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use datafusion::arrow::compute::concat_batches;
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::col;
use datafusion::prelude::SessionContext;
use crate::utils::{file_type, register_table, DfKitError, FileFormat};

pub async fn view(ctx: &SessionContext, filename: &Path, limit: Option<usize>) -> Result<(), DfKitError> {
    let df = register_table(&ctx, "t", &filename).await?;
    let limit = limit.unwrap_or(10);

    if limit > 0 {
        df.show_limit(limit).await?;
    } else {
        df.show().await?;
    }

    Ok(())
}

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

pub async fn describe(ctx: &SessionContext, filename: &Path) -> Result<(), DfKitError> {
    let df = register_table(&ctx, "t", &filename).await?;
    let describe = df.describe().await?;
    describe.show().await?;
    Ok(())
}

pub async fn schema(ctx: &SessionContext, filename: &Path) -> Result<(), DfKitError> {
    let _ = register_table(&ctx, "t", &filename).await?;
    let sql = "SELECT column_name, data_type, is_nullable \
                                FROM information_schema.columns WHERE table_name = 't'";
    let df = ctx.sql(sql).await?;
    df.show().await?;
    Ok(())
}

pub async fn count(ctx: &SessionContext, filename: &Path) -> Result<(), DfKitError> {
    let _ = register_table(&ctx, "t", &filename).await?;
    let sql = "SELECT COUNT(*) FROM t";
    let df = ctx.sql(&sql).await?;
    df.show().await?;

    Ok(())
}

pub async fn sort(
    ctx: &SessionContext,
    filename: &Path,
    columns: &[String],
    descending: bool,
    output: Option<PathBuf>,
) -> Result<(), DfKitError> {
    let df = register_table(ctx, "t", filename).await?;

    let sort_exprs = columns
        .iter()
        .map(|col_name| col(col_name).sort(!descending, descending))
        .collect();

    let sorted_df = df.sort(sort_exprs)?;

    if let Some(out_path) = output {
        let format = file_type(&out_path)?;
        match format {
            FileFormat::Csv => {
                sorted_df.write_csv(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?
            }
            FileFormat::Parquet => {
                sorted_df.write_parquet(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?
            }
            FileFormat::Json => {
                sorted_df.write_json(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?
            }
            FileFormat::Avro => {
                return Err(DfKitError::DataFusion(DataFusionError::NotImplemented("Avro write not supported".into())));
            }
        };
        println!("Sorted file written to: {}", out_path.display());
    } else {
        sorted_df.show().await?;
    }

    Ok(())
}

pub async fn reverse(
    ctx: &SessionContext,
    filename: &Path,
    output: Option<PathBuf>,
) -> Result<(), DfKitError> {
    let df = register_table(ctx, "t", filename).await?;
    let batches = df.collect().await?;

    let schema = batches[0].schema();
    let mut all_rows = vec![];
    for batch in &batches {
        for row in 0..batch.num_rows() {
            all_rows.push(batch.slice(row, 1));
        }
    }

    all_rows.reverse();

    let reversed_batch = concat_batches(&schema, &all_rows)?;

    let provider = MemTable::try_new(schema, vec![vec![reversed_batch]])?;
    ctx.register_table("reversed", Arc::new(provider))?;
    let reversed_df = ctx.table("reversed").await?;

    if let Some(out_path) = output {
        let format = file_type(&out_path)?;
        match format {
            FileFormat::Csv => {
                reversed_df
                    .write_csv(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None)
                    .await?
            }
            FileFormat::Parquet => {
                reversed_df
                    .write_parquet(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None)
                    .await?
            }
            FileFormat::Json => {
                reversed_df
                    .write_json(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None)
                    .await?
            }
            FileFormat::Avro => {
                return Err(DfKitError::DataFusion(DataFusionError::NotImplemented(
                    "Avro write not supported".into(),
                )));
            }
        };
        println!("Reversed file written to: {}", out_path.display());
    } else {
        reversed_df.show().await?;
    }

    Ok(())
}

pub async fn dfsplit(ctx: &SessionContext, filename: &Path, chunks: usize, output_dir: &Path) -> Result<(), DfKitError> {
    if chunks == 0 {
        return Err(DfKitError::CustomError("Chunks must be greater than 0".into()));
    }
    let df = register_table(ctx, "t", filename).await?;
    let total_rows = df.clone().count().await?;
    let rows_per_chunk = (total_rows + chunks - 1) / chunks;

    fs::create_dir_all(output_dir)?;

    let stem = filename.file_stem().unwrap().to_string_lossy();
    let extension = filename.extension().unwrap_or_default().to_string_lossy();
    let format = file_type(filename)?;

    for i in 0..chunks {
        let offset = i * rows_per_chunk;
        let chunk_df = df.clone().limit(offset, Some(rows_per_chunk))?;

        let chunk_filename = format!("{}_{}.{}", stem, i + 1, extension);
        let chunk_path = output_dir.join(chunk_filename);

        match format {
            FileFormat::Csv => {
                chunk_df
                    .write_csv(chunk_path.to_str().unwrap(), DataFrameWriteOptions::default(), None)
                    .await?
            }
            FileFormat::Parquet => {
                chunk_df
                    .write_parquet(chunk_path.to_str().unwrap(), DataFrameWriteOptions::default(), None)
                    .await?
            }
            FileFormat::Json => {
                chunk_df
                    .write_json(chunk_path.to_str().unwrap(), DataFrameWriteOptions::default(), None)
                    .await?
            }
            FileFormat::Avro => {
                return Err(DfKitError::DataFusion(DataFusionError::NotImplemented(
                    "Avro split write not supported".into(),
                )))
            }
        };

        println!("Written chunk {} to {}", i + 1, chunk_path.display());
    }

    Ok(())
}

pub async fn cat(ctx: &SessionContext, files: Vec<PathBuf>, out_path: &Path) -> Result<(), DfKitError> {
    let mut dfs = vec![];

    for (i, file) in files.iter().enumerate() {
        let table_name = format!("t_{}", i);
        let df = register_table(ctx, &table_name, file).await?;
        dfs.push(df);
    }

    let mut final_df = dfs.remove(0);
    for df in dfs {
        final_df = final_df.union(df)?;
    }

    let format = file_type(&out_path)?;
    match format {
        FileFormat::Csv => final_df.write_csv(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
        FileFormat::Parquet => final_df.write_parquet(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
        FileFormat::Json => final_df.write_json(out_path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?,
        FileFormat::Avro => {
            return Err(DfKitError::DataFusion(DataFusionError::NotImplemented("Avro write not supported".into())));
        }
    };
    println!("Concatenated file written to: {}", out_path.display());

    Ok(())
}
