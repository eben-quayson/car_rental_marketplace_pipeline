import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, max, min, avg, sum
from pyspark.sql.functions import unix_timestamp

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder.appName("TestSession").getOrCreate()

def test_rental_data_schema(spark):
    """Test if the rental DataFrame has the expected schema."""
    data = [("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, 50.0)]
    columns = ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"]
    
    df = spark.createDataFrame(data, columns)
    
    expected_schema = set(columns)
    actual_schema = set(df.columns)
    
    assert expected_schema == actual_schema, f"Schema mismatch! Expected: {expected_schema}, Found: {actual_schema}"

def test_total_revenue_per_location(spark):
    """Test total revenue calculation per location."""
    data = [("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, 50.0),
            ("2", "101", "501", "2024-01-02 12:00:00", "2024-01-02 13:00:00", 2, 3, 75.0),
            ("3", "102", "502", "2024-01-03 14:00:00", "2024-01-03 15:00:00", 1, 3, 25.0)]
    
    columns = ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"]
    df = spark.createDataFrame(data, columns)

    revenue_per_location = df.groupBy("pickup_location").agg(sum("total_amount").alias("total_revenue"))

    expected_data = [(1, 75.0), (2, 75.0)]
    expected_df = spark.createDataFrame(expected_data, ["pickup_location", "total_revenue"])

    assert revenue_per_location.collect() == expected_df.collect(), "Total revenue per location is incorrect!"

def test_transaction_count_per_location(spark):
    """Test if transaction count per location is correct."""
    data = [("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, 50.0),
            ("2", "101", "501", "2024-01-02 12:00:00", "2024-01-02 13:00:00", 2, 3, 75.0),
            ("3", "102", "502", "2024-01-03 14:00:00", "2024-01-03 15:00:00", 1, 3, 25.0)]
    
    columns = ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"]
    df = spark.createDataFrame(data, columns)

    transaction_count_per_location = df.groupBy("pickup_location").agg(countDistinct("rental_id").alias("transaction_count"))

    expected_data = [(1, 2), (2, 1)]
    expected_df = spark.createDataFrame(expected_data, ["pickup_location", "transaction_count"])

    assert transaction_count_per_location.collect() == expected_df.collect(), "Transaction count per location is incorrect!"

def test_average_transaction_value(spark):
    """Test calculation of the average transaction amount."""
    data = [("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, 50.0),
            ("2", "101", "501", "2024-01-02 12:00:00", "2024-01-02 13:00:00", 2, 3, 75.0),
            ("3", "102", "502", "2024-01-03 14:00:00", "2024-01-03 15:00:00", 1, 3, 25.0)]
    
    columns = ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"]
    df = spark.createDataFrame(data, columns)

    avg_transaction_value = df.agg(avg("total_amount").alias("avg_transaction")).collect()[0]["avg_transaction"]

    expected_avg = (50.0 + 75.0 + 25.0) / 3
    assert abs(avg_transaction_value - expected_avg) < 1e-6, "Average transaction value is incorrect!"

def test_rental_duration_by_vehicle_type(spark):
    """Test rental duration metrics by vehicle type."""
    rentals_data = [("1", "100", "500", "2024-01-01 10:00:00", "2024-01-01 11:00:00", 1, 2, 50.0),
                    ("2", "101", "501", "2024-01-02 12:00:00", "2024-01-02 14:00:00", 2, 3, 75.0),
                    ("3", "102", "502", "2024-01-03 14:00:00", "2024-01-03 16:30:00", 1, 3, 25.0)]
    
    rentals_columns = ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"]
    rentals_df = spark.createDataFrame(rentals_data, rentals_columns)

    vehicles_data = [("500", "Toyota", "Sedan", 2019),
                     ("501", "Honda", "SUV", 2020),
                     ("502", "Ford", "Sedan", 2021)]
    
    vehicles_columns = ["vehicle_id", "brand", "vehicle_type", "vehicle_year"]
    vehicles_df = spark.createDataFrame(vehicles_data, vehicles_columns)

    # Join rentals with vehicle info
    joined_df = rentals_df.join(vehicles_df, "vehicle_id", "left")

    # Calculate rental duration in hours
    duration_df = joined_df.withColumn("rental_duration_hours", 
                    (unix_timestamp(col("rental_end_time")) - unix_timestamp(col("rental_start_time"))) / 3600)

    avg_duration = duration_df.groupBy("vehicle_type").agg(avg("rental_duration_hours").alias("avg_duration"))

    expected_data = [("Sedan", (1 + 2.5) / 2), ("SUV", 2)]
    expected_df = spark.createDataFrame(expected_data, ["vehicle_type", "avg_duration"])

    assert avg_duration.collect() == expected_df.collect(), "Rental duration per vehicle type is incorrect!"
