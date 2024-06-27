"""
Additional test cases for the ingest.py script
"""
import os
import subprocess
import time

import duckdb
import pytest


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")


def run_ingestion(file_path="uncommitted/votes.jsonl") -> float:
    """
    Returns time in seconds that the ingestion process took to run
    """
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "coffeebeans_dataeng_exercise.ingest",
            file_path,
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    return toc - tic


def test_check_schema_creation():
    run_ingestion()
    sql = """
        SELECT schema_name 
        FROM information_schema.schemata
        WHERE schema_name='blog_analysis';
    """
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.sql(sql)
    assert len(result.fetchall()) == 1, "Expected schema 'blog_analysis' to exist"


def test_check_table_columns():
    run_ingestion()
    sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name='votes' AND table_schema='blog_analysis';
    """
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.sql(sql)
    columns = result.fetchall()
    expected_columns = [
        ("Id", "INTEGER"),
        ("PostId", "INTEGER"),
        ("VoteTypeId", "INTEGER"),
        ("CreationDate", "TIMESTAMP")
    ]
    assert len(columns) == len(expected_columns), "Expected all columns to be present"
    for column in expected_columns:
        assert column in columns, f"Expected column {column} in the table"


def test_incorrect_file_path():
    with pytest.raises(subprocess.CalledProcessError):
        run_ingestion("uncommitted/non_existent_file.jsonl")


def test_ingestion_with_missing_columns():
    # Create a temporary file with missing columns
    temp_file_path = "uncommitted/votes_missing_columns.jsonl"
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        temp_file.write('{"Id":"18","PostId":"15","CreationDate":"2022-03-01T00:00:00.000"}\n')

    run_ingestion(temp_file_path)

    sql = "SELECT COUNT(*) FROM blog_analysis.votes WHERE Id = 18"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert count_in_db == 1, "Expected row with missing columns to be ingested with default values"

    # Clean up
    os.remove(temp_file_path)


def test_ingestion_with_extra_columns():
    # Create a temporary file with extra columns
    temp_file_path = "uncommitted/votes_extra_columns.jsonl"
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        temp_file.write('{"Id":"19","PostId":"16","VoteTypeId":"3","CreationDate":"2022-03-02T00:00:00.000","ExtraColumn":"extra_value"}\n')#Extra column will also be inserted as it will be a part of schema as only 1 rows means 100% threshold criteria met.

    run_ingestion(temp_file_path)

    sql = "SELECT COUNT(*) FROM blog_analysis.votes WHERE Id = 19"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert count_in_db == 1, "Expected row with extra columns to be ingested while ignoring extra columns"

    # Clean up
    os.remove(temp_file_path)


def test_correct_primary_key_assignment():
    run_ingestion()
    sql = "PRAGMA table_info('blog_analysis.votes')"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    columns_info = result.fetchall()
    primary_key_column = [col for col in columns_info if col[5] == 1]
    assert len(primary_key_column) == 1, "Expected one primary key column"
    assert primary_key_column[0][1] == 'Id', "Expected 'Id' to be the primary key column"
  

def test_inconsistent_data_types():
    # Create a temporary file with inconsistent data types
    temp_file_path = "uncommitted/votes_inconsistent_data_types.jsonl"
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        temp_file.write('{"Id":"20","PostId":"17","VoteTypeId":"three","CreationDate":"2022-03-03T00:00:00.000"}\n') 

    run_ingestion(temp_file_path)

    sql = "SELECT COUNT(*) FROM blog_analysis.votes WHERE Id = 20"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert count_in_db == 0, "Expected row with inconsistent data types to be skipped"

    # Clean up
    os.remove(temp_file_path)

def test_empty_file():
    # Create a temporary empty file
    temp_file_path = "uncommitted/empty_votes.jsonl"
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        pass

    run_ingestion(temp_file_path)

    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert count_in_db == 0, "Expected no rows to be ingested from an empty file"

    # Clean up
    os.remove(temp_file_path)


def test_mixed_data_types():
    # Create a temporary file with mixed data types
    temp_file_path = "uncommitted/votes_mixed_data_types.jsonl"
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        temp_file.write('{"Id":"21","PostId":"18","VoteTypeId":"2","CreationDate":"2022-03-04T00:00:00.000"}\n')
        temp_file.write('{"Id":"22","PostId":"18","VoteTypeId":"two","CreationDate":"2022-03-04T00:00:00.000"}\n')

    run_ingestion(temp_file_path)

    sql = "SELECT COUNT(*) FROM blog_analysis.votes WHERE Id IN (21, 22)"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert count_in_db == 1, "Expected only valid rows to be ingested from file with mixed data types"

    # Clean up
    os.remove(temp_file_path)


def test_non_utf8_file():
    # Create a temporary file with non-UTF-8 encoding
    temp_file_path = "uncommitted/votes_non_utf8.jsonl"
    with open(temp_file_path, 'w', encoding='latin-1') as temp_file:
        temp_file.write('{"Id":"23","PostId":"19","VoteTypeId":"2","CreationDate":"2022-03-05T00:00:00.000"}\n')

    with pytest.raises(subprocess.CalledProcessError):
        run_ingestion(temp_file_path)

    sql = "SELECT COUNT(*) FROM blog_analysis.votes WHERE Id = 23"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert count_in_db == 0, "Expected no rows to be ingested from a non-UTF-8 file"

    # Clean up
    os.remove(temp_file_path)


def test_large_file():
    # Create a large temporary file
    temp_file_path = "uncommitted/votes_large.jsonl"
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        for i in range(1, 100001): #Increase the row numbers to test for Big File
            temp_file.write(f'{{"Id":"{i}","PostId":"{i%10}","VoteTypeId":"{i%3}","CreationDate":"2022-03-05T00:00:00.000"}}\n')

    time_taken_seconds = run_ingestion(temp_file_path)

    assert time_taken_seconds < 60, "Ingestion of large file is too slow!"

    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert count_in_db == 100000, "Expected all rows to be ingested from the large file"

    # Clean up
    os.remove(temp_file_path)