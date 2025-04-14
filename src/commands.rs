use std::path::{Path, PathBuf};
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrameWriteOptions;
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
    let _ = register_table(ctx, "t", filename).await?;
    let df = ctx.table("t").await?;

    let sort_exprs = columns
        .iter()
        .map(|col_name| {
            if descending {
                col(col_name).sort(false, true)
            } else {
                col(col_name).sort(true, false)
            }
        })
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
