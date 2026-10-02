import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_clean_data")
        .getOrCreate()
    )


def test_valid_records_are_kept(spark):
    data = [
        ("Alice", 100),
        ("Bob", 50),
    ]

    df = spark.createDataFrame(data, ["name", "amount"])
    result = clean_data(df)

    assert result.count() == 2


def test_records_with_non_positive_amount_are_removed(spark):
    data = [
        ("Alice", 100),
        ("Bob", 0),
        ("Charlie", -10),
    ]

    df = spark.createDataFrame(data, ["name", "amount"])
    result = clean_data(df)

    assert result.count() == 1


def test_records_with_null_names_are_removed(spark):
    data = [
        ("Alice", 100),
        (None, 200),
    ]

    df = spark.createDataFrame(data, ["name", "amount"])
    result = clean_data(df)

    assert result.count() == 1


def test_amount_with_tax_is_calculated_correctly(spark):
    data = [
        ("Alice", 100),
    ]

    df = spark.createDataFrame(data, ["name", "amount"])
    result = clean_data(df)

    row = result.first()

    assert row["amount_with_tax"] == pytest.approx(120.0)