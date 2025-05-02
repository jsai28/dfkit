![CI](https://github.com/jsai28/dfkit/actions/workflows/ci.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/dfkit)](https://crates.io/crates/dfkit)
![License](https://img.shields.io/github/license/jsai28/dfkit)

## dfkit
dfkit is an extensive suite of command-line functions to easily view, query, and manipulate CSV, Parquet, JSON, and Avro files. Written in Rust and powered by [Apache Arrow](https://github.com/apache/arrow) and [Apache DataFusion](https://github.com/apache/datafusion). Currently a work in progress.

## Highlights
Here's a high level overview of some of the features in dfkit:

- Supports viewing and manipulating files stored locally, from URLs, and S3 storage.
- Works with CSV, JSON, Parquet, and Avro files
- Ultra-fast performance powered by Apache Arrow and DataFusion
- Transform data with SQL or with several other built-in functions
- Written entirely in Rust!
## Commands
```
dfkit 0.2.0

USAGE:
    dfkit <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    cat         Concatenate multiple files or all files in a directory
    convert     Convert file format (CSV, Parquet, JSON)
    count       Count the number of rows in a file
    dedup       Remove duplicate rows
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
dfkit view sample.csv

+-------+-----+
| name  | age |
+-------+-----+
| Joe   | 34  |
| Matt  | 24  |
| Emily | 65  |
+-------+-----+
```
Query allows you to query the data with SQL. An optional output argument can also be supplied to save the results.
```
dfkit query sample.csv --sql "SELECT * FROM t WHERE age < 50"

+------+-----+
| name | age |
+------+-----+
| Joe  | 34  |
| Matt | 24  |
+------+-----+
```
Show the file schema.
```
dfkit schema sample.csv

+-------------+-----------+-------------+
| column_name | data_type | is_nullable |
+-------------+-----------+-------------+
| name        | Utf8      | YES         |
| age         | Int64     | YES         |
+-------------+-----------+-------------+
```
Show summary statistics of a file with `describe`.
```
dfkit describe sample.csv

+------------+-------+-------------------+
| describe   | name  | age               |
+------------+-------+-------------------+
| count      | 3     | 3.0               |
| null_count | 0     | 0.0               |
| mean       | null  | 41.0              |
| std        | null  | 21.37755832643195 |
| min        | Emily | 24.0              |
| max        | Matt  | 65.0              |
| median     | null  | 34.0              |
+------------+-------+-------------------+
```
Reverse the order of rows (save the output with --output)
```
dfkit reverse sample.csv

+-------+-----+
| name  | age |
+-------+-----+
| Emily | 65  |
| Matt  | 24  |
| Joe   | 34  |
+-------+-----+
```
Sort rows and optionally save the output with --output. You can specify multiple columns as 
a comma separated string.
```
dfkit sort sample.csv --columns "age"

+-------+-----+
| name  | age |
+-------+-----+
| Matt  | 24  |
| Joe   | 34  |
| Emily | 65  |
+-------+-----+
```

