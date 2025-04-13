use std::path::{Path, PathBuf};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use crate::utils::{register_table, DfKitError};

pub async fn query(ctx: &SessionContext, filename: &Path, sql: Option<String>, output: Option<PathBuf>) -> Result<(), DfKitError> {
    let _ = register_table(&ctx, "t", &filename).await?;
    let df_sql = ctx.sql(&*sql.unwrap()).await?;

    if let Some(path) = output {
        println!("Writing output to {}", path.display());
        df_sql.write_csv(path.to_str().unwrap(), DataFrameWriteOptions::default(), None).await?;
    } else {
        df_sql.show().await?;
    }

    Ok(())
}