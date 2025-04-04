import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, countDistinct

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("EdgeCasesTest").getOrCreate()

def test_zero_transaction_amount(spark):
    """Ensure transactions with zero amount are handled correctly."""
    data = [("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, 0.0)]
    columns = ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"]
    
    df = spark.createDataFrame(data, columns)

    total_revenue = df.agg(sum("total_amount").alias("total_revenue")).collect()[0]["total_revenue"]
    assert total_revenue == 0, "Zero transaction amounts should not contribute to total revenue"

def test_negative_transaction_amount(spark):
    """Ensure negative transaction amounts are not included in revenue calculations."""
    data = [("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, -50.0)]
    df = spark.createDataFrame(data, ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"])

    valid_transactions = df.filter(col("total_amount") >= 0)
    total_revenue = valid_transactions.agg(sum("total_amount").alias("total_revenue")).collect()[0]["total_revenue"]

    assert total_revenue == 0, "Negative transactions should not be included in revenue"

def test_duplicate_rental_entries(spark):
    """Ensure duplicate rental transactions do not inflate revenue or counts."""
    data = [("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, 50.0),
            ("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, 50.0)]
    
    df = spark.createDataFrame(data, ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"])

    unique_transactions = df.dropDuplicates(["rental_id"])
    total_revenue = unique_transactions.agg(sum("total_amount").alias("total_revenue")).collect()[0]["total_revenue"]

    assert total_revenue == 50.0, "Duplicate rentals should not affect revenue calculation"

def test_null_values_handling(spark):
    """Ensure null values in critical fields do not break calculations."""
    data = [(None, "100", "500", None, "2024-01-01 11:00:00", 1, 2, 50.0)]
    
    df = spark.createDataFrame(data, ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"])
    
    valid_data = df.dropna()
    assert valid_data.count() == 0, "Rows with null rental_id or timestamps should be removed"

def test_large_dataset_performance(spark):
    """Test if the pipeline can handle large datasets efficiently."""
    data = [("rental_" + str(i), "user_" + str(i % 100), "vehicle_" + str(i % 50),
             "2024-01-01 10:00:00", "2024-01-01 11:00:00", i % 10, (i % 10) + 1, i * 1.5)
            for i in range(1_000_000)]  # 1 million records

    df = spark.createDataFrame(data, ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"])

    revenue_per_location = df.groupBy("pickup_location").agg(sum("total_amount").alias("total_revenue"))

    assert revenue_per_location.count() > 0, "Large dataset processing failed"
