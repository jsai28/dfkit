use assert_cmd::Command;
use tempfile::tempdir;
use std::fs;
use std::path::{Path, PathBuf};
use insta::assert_snapshot;
use dfkit::utils::parse_file_list;

fn write_temp_file(dir: &Path, name: &str, contents: &str) -> std::path::PathBuf {
    let file_path = dir.join(name);
    fs::write(&file_path, contents).unwrap();
    file_path
}

fn create_basic_csv(dir: &Path) -> PathBuf {
    write_temp_file(dir, "input.csv", "name,age\nalice,30\nbob,40\n")
}

fn create_extended_csv(dir: &Path) -> PathBuf {
    write_temp_file(dir, "input.csv", "name,age\nalice,30\nbob,40\ncharlie,50\n")
}

#[test]
fn test_view() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());
    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args(["view", input.to_str().unwrap()]);
    let output = cmd.assert().success().get_output().stdout.clone();
    assert_snapshot!(String::from_utf8(output).unwrap(), @r"
    +-------+-----+
    | name  | age |
    +-------+-----+
    | alice | 30  |
    | bob   | 40  |
    +-------+-----+
    ");
}
#[test]
fn test_view_with_limit() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());
    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args(["view", input.to_str().unwrap(), "-l", "1"]);
    let output = cmd.assert().success().get_output().stdout.clone();
    assert_snapshot!(String::from_utf8(output).unwrap(), @r"
    +-------+-----+
    | name  | age |
    +-------+-----+
    | alice | 30  |
    +-------+-----+
    ");
}

#[test]
fn test_query() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());
    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args(["query", input.to_str().unwrap(), "--sql", "SELECT * FROM t WHERE age > 35"]);
    let output = cmd.assert().success().get_output().stdout.clone();
    assert_snapshot!(String::from_utf8(output).unwrap(), @r"
    +------+-----+
    | name | age |
    +------+-----+
    | bob  | 40  |
    +------+-----+
    ");
}

#[test]
fn test_query_with_output() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());
    let output_path = temp.path().join("out.csv");

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "query",
        input.to_str().unwrap(),
        "--sql",
        "SELECT * FROM t WHERE age > 35",
        "--output",
        output_path.to_str().unwrap(),
    ]);

    cmd.assert().success();

    assert!(output_path.exists(), "Output file was not created");
}

#[test]
fn test_convert_csv_to_json() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());
    let output = temp.path().join("output.json");

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "convert",
        input.to_str().unwrap(),
        output.to_str().unwrap(),
    ]);

    cmd.assert().success();

    let output_contents = fs::read_to_string(&output).unwrap();
    assert!(output_contents.contains("\"name\":\"alice\"") || output_contents.contains("alice")); // Loose check
}

#[test]
fn test_convert_csv_to_parquet() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());
    let output = temp.path().join("output.parquet");

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "convert",
        input.to_str().unwrap(),
        output.to_str().unwrap(),
    ]);

    cmd.assert().success();
    assert!(output.exists(), "Parquet file not created");
}

#[test]
fn test_convert_to_avro_should_fail() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());
    let output = temp.path().join("output.avro");

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "convert",
        input.to_str().unwrap(),
        output.to_str().unwrap(),
    ]);

    cmd.assert()
        .failure()
        .stderr(predicates::str::contains("Avro write not supported"));
}

#[test]
fn test_describe_command() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "describe",
        input.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    assert_snapshot!(String::from_utf8(output).unwrap(), @r"
    +------------+-------+--------------------+
    | describe   | name  | age                |
    +------------+-------+--------------------+
    | count      | 2     | 2.0                |
    | null_count | 0     | 0.0                |
    | mean       | null  | 35.0               |
    | std        | null  | 7.0710678118654755 |
    | min        | alice | 30.0               |
    | max        | bob   | 40.0               |
    | median     | null  | 35.0               |
    +------------+-------+--------------------+
    ");
}

#[test]
fn test_schema_command() {
    let temp = tempdir().unwrap();
    let input = create_basic_csv(temp.path());

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "schema",
        input.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    assert_snapshot!(String::from_utf8(output).unwrap(), @r"
    +-------------+-----------+-------------+
    | column_name | data_type | is_nullable |
    +-------------+-----------+-------------+
    | name        | Utf8      | YES         |
    | age         | Int64     | YES         |
    +-------------+-----------+-------------+
    ");
}

#[test]
fn test_count_command() {
    let temp = tempdir().unwrap();
    let input = create_extended_csv(temp.path());

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "count",
        input.to_str().unwrap(),
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    assert_snapshot!(String::from_utf8(output).unwrap(), @r"
    +----------+
    | count(*) |
    +----------+
    | 3        |
    +----------+
    ");
}

#[test]
fn test_sort_command_ascending() {
    let temp = tempdir().unwrap();
    let input = create_extended_csv(temp.path());

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "sort",
        input.to_str().unwrap(),
        "--columns", "age",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    assert_snapshot!(String::from_utf8(output).unwrap(), @r"
    +---------+-----+
    | name    | age |
    +---------+-----+
    | alice   | 30  |
    | bob     | 40  |
    | charlie | 50  |
    +---------+-----+
    ");
}

#[test]
fn test_sort_command_descending() {
    let temp = tempdir().unwrap();
    let input = create_extended_csv(temp.path());

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    cmd.args([
        "sort",
        input.to_str().unwrap(),
        "--columns", "age",
        "--descending",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    assert_snapshot!(String::from_utf8(output).unwrap(), @r"
    +---------+-----+
    | name    | age |
    +---------+-----+
    | charlie | 50  |
    | bob     | 40  |
    | alice   | 30  |
    +---------+-----+
    ");
}

#[test]
fn test_reverse_stdout() {
    let temp = tempdir().unwrap();
    let input_path = create_extended_csv(temp.path());
    fs::write(&input_path, "name,age\nalice,30\nbob,40\ncharlie,25").unwrap();

    let mut cmd = Command::cargo_bin("dfkit").unwrap();
    let output = cmd
        .args([
            "reverse",
            input_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    assert_snapshot!(String::from_utf8(output).unwrap(), @r###"
    +---------+-----+
    | name    | age |
    +---------+-----+
    | charlie | 25  |
    | bob     | 40  |
    | alice   | 30  |
    +---------+-----+
    "###);
}

#[test]
fn test_split_creates_chunks() {
    let temp = tempdir().unwrap();
    let input_path = temp.path().join("data.csv");
    let output_dir = temp.path().join("out");

    // Write input file
    fs::write(
        &input_path,
        "name,age\nalice,30\nbob,40\ncharlie,25\ndave,20\nellen,45\n",
    )
        .unwrap();

    // Run the CLI command
    let _ = Command::cargo_bin("dfkit")
        .unwrap()
        .args(&[
            "split",
            input_path.to_str().unwrap(),
            "--chunks",
            "2",
            output_dir.to_str().unwrap(),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    // Assert output files exist using parse_file_list
    let mut files= parse_file_list(None, Some(output_dir.clone())).unwrap();

    files.sort();

    assert_eq!(files.len(), 2);
}

#[test]
fn test_cat_concatenates_csv_files() {
    let temp = tempdir().unwrap();
    let file1 = temp.path().join("part1.csv");
    let file2 = temp.path().join("part2.csv");
    let out_file = temp.path().join("combined.csv");

    // Create sample CSV files
    fs::write(&file1, "name,age\nalice,30\nbob,40\n").unwrap();
    fs::write(&file2, "name,age\ncharlie,25\ndave,20\n").unwrap();

    let input_files = format!("{},{}", file1.display(), file2.display());

    // Run the CLI command
    let _ = Command::cargo_bin("dfkit")
        .unwrap()
        .args(&[
            "cat",
            "--files",
            &input_files,
            "--output",
            out_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Read and sort the output CSV
    let result_csv = fs::read_to_string(&out_file).unwrap();
    let lines: Vec<&str> = result_csv.lines().collect();

    let header = lines[0];
    let mut records = lines[1..].to_vec();
    records.sort(); // Sort records alphabetically

    let sorted_result = std::iter::once(header)
        .chain(records.into_iter())
        .collect::<Vec<_>>()
        .join("\n");

    assert_snapshot!(sorted_result, @r"
    name,age
    alice,30
    bob,40
    charlie,25
    dave,20
    ");
}
