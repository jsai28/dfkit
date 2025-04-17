![CI](https://github.com/jsai28/dfkit/actions/workflows/ci.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/dfkit)](https://crates.io/crates/dfkit)

## dfkit
dfkit is an extensive suite of command-line functions to easily view, query, and manipulate CSV, Parquet, JSON, and Avro files. Written in Rust and powered by [Apache Arrow](https://github.com/apache/arrow) and [Apache DataFusion](https://github.com/apache/datafusion). Currently a work in progress.

## Commands
```
dfkit 0.1.0

USAGE:
    dfkit <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    cat         Concatenate multiple files or all files in a directory
    convert     Convert file format (CSV, Parquet, JSON)
    count       Count the number of rows in a file
    describe    Show summary statistics for a file
    help        Prints this message or the help of the given subcommand(s)
    query       Run a SQL query on a file
    reverse     Reverse the order of rows
    schema      Show schema of a file
    sort        Sort rows by one or more columns
    split       Split a file into N chunks
    view        View the contents of a file

```
## Installation
dfkit can be installed via cargo (requires rust):
```
cargo install dfkit
```
## Examples

View takes the filename and an optional limit argument.
```
dfkit view sample.csv --limit 1
```
```
+------+-----+
| Name | Age |
+------+-----+
| Joe  | 34  |
+------+-----+
```
