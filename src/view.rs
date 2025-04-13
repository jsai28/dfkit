use std::path::{Path};
use datafusion::prelude::SessionContext;
use crate::utils::{register_table, DfKitError};

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