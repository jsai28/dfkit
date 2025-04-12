use dfkit::utils::{register_table, DfKitError};
use structopt::StructOpt;
use std::path::PathBuf;
use datafusion::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "dfkit", about = "Command-line data toolkit")]
enum Commands {
    View {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short = "l", long = "limit")]
        limit: Option<usize>,
    },
}

#[tokio::main]
async fn main() -> Result<(), DfKitError> {
    let cmd = Commands::from_args();
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    match cmd {
        Commands::View { filename, limit } => {
            let df = register_table(&ctx, "t", &filename).await?;
            let limit = limit.unwrap_or(10);
            df.show_limit(limit).await?;
        }
    }

    Ok(())
}
