use dfkit::utils::{DfKitError};
use structopt::StructOpt;
use std::path::PathBuf;
use datafusion::prelude::*;
use dfkit::commands::{view, query, convert, describe, schema, count};

#[derive(Debug, StructOpt)]
#[structopt(name = "dfkit", about = "Command-line data toolkit")]
enum Commands {
    View {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short = "l", long = "limit")]
        limit: Option<usize>,
    },
    Query {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short = "s", long = "sql")]
        sql: Option<String>,
        #[structopt(short = "o", long = "output", parse(from_os_str))]
        output: Option<PathBuf>,
    },
    Convert {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(parse(from_os_str))]
        output_filename: PathBuf,
    },
    Describe {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    },
    Schema {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    },
    Count {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    }
}

#[tokio::main]
async fn main() -> Result<(), DfKitError> {
    let cmd = Commands::from_args();
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    match cmd {
        Commands::View { filename, limit } => {
            view(&ctx, &filename, limit).await?;
        }
        Commands::Query { filename, sql , output} => {
            query(&ctx, &filename, sql, output).await?;
        }
        Commands::Convert { filename, output_filename } => {
            convert(&ctx, &filename, &output_filename).await?;
        }
        Commands::Describe { filename } => {
            describe(&ctx, &filename).await?;
        }
        Commands::Schema { filename } => {
            schema(&ctx, &filename).await?;
        }
        Commands::Count { filename } => {
            count(&ctx, &filename).await?;
        }
    }

    Ok(())
}
