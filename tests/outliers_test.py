"""
Additional test cases for the outliers.py script
"""
import subprocess
import os

import duckdb
import pytest


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")


def run_outliers_calculation():
    result = subprocess.run(
        args=["python", "-m", "coffeebeans_dataeng_exercise.outliers"],
        capture_output=True,
    )
    result.check_returncode()


def test_table_not_existing():
    # Drop the votes table to simulate a non-existing table
    con = duckdb.connect("warehouse.db")
    con.execute("DROP TABLE blog_analysis.votes")
    con.close()

    with pytest.raises(subprocess.CalledProcessError):
        run_outliers_calculation()


def test_empty_table():
    # Ensure the votes table is empty
    run_outliers_calculation()
    sql = "SELECT COUNT(*) FROM blog_analysis.outlier_weeks"
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        count_in_view = result.fetchall()[0][0]
        assert count_in_view == 0, "Expected view 'outlier_weeks' to be empty for empty votes table"
    finally:
        con.close()


def test_consistent_data():
    con = duckdb.connect("warehouse.db")
    con.execute("""
    INSERT INTO blog_analysis.votes (Id, PostId, VoteTypeId, CreationDate) VALUES
    (1, 1, 1, '2022-01-01T00:00:00.000'),
    (2, 2, 1, '2022-01-08T00:00:00.000'),
    (3, 3, 1, '2022-01-15T00:00:00.000')
    """)
    con.close()

    run_outliers_calculation()
    sql = "SELECT COUNT(*) FROM blog_analysis.outlier_weeks"
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        count_in_view = result.fetchall()[0][0]
        assert count_in_view == 0, "Expected view 'outlier_weeks' to be empty for consistent data"
    finally:
        con.close()


def test_missing_weeks():
    con = duckdb.connect("warehouse.db")
    con.execute("""
    INSERT INTO blog_analysis.votes (Id, PostId, VoteTypeId, CreationDate) VALUES
    (1, 1, 1, '2022-01-01T00:00:00.000'),
    (2, 2, 1, '2022-01-22T00:00:00.000'),
    (3, 3, 1, '2022-02-12T00:00:00.000')
    """)
    con.close()

    run_outliers_calculation()
    sql = "SELECT Year, WeekNumber FROM blog_analysis.outlier_weeks"
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        outlier_weeks = result.fetchall()
        expected_outliers = [(2022, 3), (2022, 5)]
        for outlier in expected_outliers:
            assert outlier in outlier_weeks, f"Expected week {outlier} to be an outlier due to missing data"
    finally:
        con.close()


def test_threshold_logic():
    con = duckdb.connect("warehouse.db")
    con.execute("""
    INSERT INTO blog_analysis.votes (Id, PostId, VoteTypeId, CreationDate) VALUES
    (1, 1, 1, '2022-01-01T00:00:00.000'),
    (2, 1, 1, '2022-01-01T00:00:00.000'),
    (3, 1, 1, '2022-01-01T00:00:00.000'),
    (4, 2, 1, '2022-01-08T00:00:00.000'),
    (5, 2, 1, '2022-01-08T00:00:00.000'),
    (6, 2, 1, '2022-01-08T00:00:00.000'),
    (7, 3, 1, '2022-01-15T00:00:00.000')
    """)
    con.close()

    run_outliers_calculation()
    sql = "SELECT Year, WeekNumber FROM blog_analysis.outlier_weeks"
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        outlier_weeks = result.fetchall()
        expected_outliers = [(2022, 2), (2022, 3)]
        for outlier in expected_outliers:
            assert outlier in outlier_weeks, f"Expected week {outlier} to be an outlier based on threshold logic"
    finally:
        con.close()