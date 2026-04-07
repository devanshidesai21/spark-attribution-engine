from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="AttributionEngine"):
    """Initialize and return a SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def run_attribution_model(df):
    """
    Calculates First-Touch and Last-Touch attribution revenue per channel.
    Expects columns: user_id, timestamp, event_type, channel, revenue
    """
    logger.info("Starting Attribution Calculation...")

    # 1. Isolate Conversions
    conversions = df.filter(F.col("event_type") == "conversion") \
        .select(
            "user_id", 
            F.col("timestamp").alias("conversion_time"), 
            "revenue"
        )

    # 2. Isolate Marketing Touchpoints (Impressions/Clicks)
    touchpoints = df.filter(F.col("event_type").isin("impression", "click"))

    # 3. Join and filter out touchpoints that happened AFTER the conversion
    # Using an inner join ensures we only attribute revenue to users who actually converted
    valid_paths = touchpoints.join(conversions, "user_id", "inner") \
        .filter(F.col("timestamp") <= F.col("conversion_time"))

    # 4. Define Window Specifications
    # Partition by user and conversion time to handle multiple conversions per user safely
    window_asc = Window.partitionBy("user_id", "conversion_time").orderBy("timestamp")
    window_desc = Window.partitionBy("user_id", "conversion_time").orderBy(F.desc("timestamp"))

    # 5. Rank the touchpoints
    ranked_paths = valid_paths \
        .withColumn("first_touch_rank", F.row_number().over(window_asc)) \
        .withColumn("last_touch_rank", F.row_number().over(window_desc))

    # 6. Calculate First-Touch Attribution
    first_touch_df = ranked_paths.filter(F.col("first_touch_rank") == 1) \
        .groupBy("channel") \
        .agg(F.sum("revenue").alias("first_touch_revenue")) \
        .withColumn("model", F.lit("First-Touch"))

    # 7. Calculate Last-Touch Attribution
    last_touch_df = ranked_paths.filter(F.col("last_touch_rank") == 1) \
        .groupBy("channel") \
        .agg(F.sum("revenue").alias("last_touch_revenue")) \
        .withColumn("model", F.lit("Last-Touch"))

    logger.info("Attribution Calculation Complete.")
    return first_touch_df, last_touch_df

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Example Dummy Data Creation for testing the logic locally
    data = [
        ("u1", "2023-10-01 10:00:00", "impression", "Social Media", 0.0),
        ("u1", "2023-10-02 11:00:00", "click", "Organic Search", 0.0),
        ("u1", "2023-10-03 12:00:00", "conversion", "Direct", 150.0),
        ("u2", "2023-10-01 09:00:00", "impression", "Paid Search", 0.0),
        ("u2", "2023-10-05 14:00:00", "conversion", "Direct", 200.0)
    ]
    
    columns = ["user_id", "timestamp", "event_type", "channel", "revenue"]
    raw_df = spark.createDataFrame(data, columns)
    
    first_touch, last_touch = run_attribution_model(raw_df)
    
    print("--- First Touch Attribution ---")
    first_touch.show()
    
    print("--- Last Touch Attribution ---")
    last_touch.show()
