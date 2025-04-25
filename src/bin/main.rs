use datafusion::prelude::*;
use dfkit::commands::{cat, convert, count, describe, dfsplit, query, reverse, schema, sort, view, dedup};
use dfkit::utils::{DfKitError, parse_file_list};
use std::env;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "dfkit",
    about = "A fast SQL-based CLI tool for working with CSV, Parquet, and JSON data files."
)]
pub struct Cli {
    #[structopt(subcommand)]
    pub command: Commands,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "dfkit")]
pub enum Commands {
    #[structopt(about = "View the contents of a file")]
    View {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short = "l", long = "limit")]
        limit: Option<usize>,
    },

    #[structopt(about = "Run a SQL query on a file")]
    Query {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short = "s", long = "sql")]
        sql: Option<String>,
        #[structopt(short = "o", long = "output", parse(from_os_str))]
        output: Option<PathBuf>,
    },

    #[structopt(about = "Convert file format (CSV, Parquet, JSON)")]
    Convert {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(parse(from_os_str))]
        output: PathBuf,
    },

    #[structopt(about = "Show summary statistics for a file")]
    Describe {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    },

    #[structopt(about = "Show schema of a file")]
    Schema {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    },

    #[structopt(about = "Count the number of rows in a file")]
    Count {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    },

    #[structopt(about = "Sort rows by one or more columns")]
    Sort {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short, long, use_delimiter = true)]
        columns: Vec<String>,
        #[structopt(short, long)]
        descending: bool,
        #[structopt(short = "o", long = "output", parse(from_os_str))]
        output: Option<PathBuf>,
    },

    #[structopt(about = "Reverse the order of rows")]
    Reverse {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short = "o", long = "output", parse(from_os_str))]
        output: Option<PathBuf>,
    },

    #[structopt(about = "Split a file into N chunks")]
    Split {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short, long)]
        chunks: usize,
        #[structopt(short, long)]
        output: Option<PathBuf>,
    },

    #[structopt(about = "Concatenate multiple files or all files in a directory row-wise")]
    Cat {
        #[structopt(long, required_unless = "dir")]
        files: Option<String>,
        #[structopt(long, required_unless = "files")]
        dir: Option<PathBuf>,
        #[structopt(short, long, parse(from_os_str))]
        output: PathBuf,
    },

    #[structopt(about = "Remove duplicate rows")]
    Dedup {
        #[structopt(short, long, parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short, long, parse(from_os_str))]
        output: Option<PathBuf>,
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
        Commands::Query {
            filename,
            sql,
            output,
        } => {
            query(&ctx, &filename, sql, output).await?;
        }
        Commands::Convert {
            filename,
            output,
        } => {
            convert(&ctx, &filename, &output).await?;
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
        Commands::Sort {
            filename,
            columns,
            descending,
            output,
        } => {
            sort(&ctx, &filename, &columns, descending, output).await?;
        }
        Commands::Reverse { filename, output } => {
            reverse(&ctx, &filename, output).await?;
        }
        Commands::Split {
            filename,
            chunks,
            output,
        } => {
            let out_dir = output.unwrap_or_else(|| env::current_dir().unwrap());
            dfsplit(&ctx, &filename, chunks, &out_dir).await?;
        }
        Commands::Cat { files, dir, output } => {
            let file_list = parse_file_list(files, dir)?;
            cat(&ctx, file_list, &output).await?;
        }
        Commands::Dedup { filename, output } => {
            dedup(&ctx, &filename, output).await?;
        }
    }

    Ok(())
}
