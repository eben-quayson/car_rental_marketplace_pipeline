import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestRentalMarketplace").master("local[*]").getOrCreate()

def test_revenue_per_location_schema(spark):
    # Sample mock data
    rentals = spark.createDataFrame([
        (1, "LOC1", 100.0),
        (2, "LOC1", 150.0),
        (3, "LOC2", 200.0),
    ], ["rental_id", "pickup_location", "total_amount"])

    locations = spark.createDataFrame([
        ("LOC1", "Accra"),
        ("LOC2", "Kumasi")
    ], ["location_id", "location_name"])

    # Simulate revenue per location logic
    revenue = rentals.groupBy("pickup_location") \
        .sum("total_amount") \
        .join(locations, rentals.pickup_location == locations.location_id, "left") \
        .select("location_name", "sum(total_amount)").withColumnRenamed("sum(total_amount)", "total_revenue")

    assert "location_name" in revenue.columns
    assert "total_revenue" in revenue.columns
    assert revenue.count() == 2
