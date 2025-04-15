use assert_cmd::Command;
use tempfile::tempdir;
use std::fs;
use std::path::Path;
use insta::assert_snapshot;

fn write_temp_file(dir: &Path, name: &str, contents: &str) -> std::path::PathBuf {
    let file_path = dir.join(name);
    fs::write(&file_path, contents).unwrap();
    file_path
}

#[test]
fn test_view() {
    let temp = tempdir().unwrap();
    let input = write_temp_file(temp.path(), "input.csv", "name,age\nalice,30\nbob,40\n");
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
    let input = write_temp_file(temp.path(), "input.csv", "name,age\nalice,30\nbob,40\n");
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
