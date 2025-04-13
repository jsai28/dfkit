use std::path::Path;
use datafusion::prelude::SessionContext;
use crate::utils::{register_table, DfKitError};

pub async fn describe(ctx: &SessionContext, filename: &Path) -> Result<(), DfKitError> {
    let df = register_table(&ctx, "t", &filename).await?;
    let describe = df.describe().await?;
    describe.show().await?;
    Ok(())
}