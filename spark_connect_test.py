from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Tip: Change the app name for each instance to track them in the Spark UI
remote_url = "sc://localhost:15002"

try:
    spark = SparkSession.builder \
        .remote(remote_url) \
        .appName("Concurrent_Stress_Test_App_2") \
        .getOrCreate()

    # spark.sql("CREATE DATABASE IF NOT EXISTS raw")
    spark.sql("USE raw")

    # ---------------------------
    # BRONZE (Sensor Data)
    # ---------------------------
    # Using 'sensor_readings' instead of 'meter_readings'
    print("Writing: raw.sensor_readings (Bronze)")

    N = 10_000_000 

    bronze_df = spark.range(0, N).select(
        F.col("id").alias("sensor_id"),
        (F.rand() * 100).alias("metric_value"),
        F.current_timestamp().alias("ingest_ts"),
        (F.col("id") % 10).alias("zone_id"),
        F.expr("uuid()").alias("event_id")
    )

    bronze_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("zone_id") \
        .saveAsTable("sensor_readings")

    # ---------------------------
    # SILVER (Join + Transform)
    # ---------------------------
    # Using 'sensor_cleaned' instead of 'meter_cleaned'
    print("Writing: raw.sensor_cleaned (Silver)")

    dim_df = spark.range(0, 10).select(
        F.col("id").alias("zone_id"),
        F.concat(F.lit("Zone-"), F.col("id")).alias("zone_name")
    )

    silver_df = spark.read.table("sensor_readings") \
        .filter("metric_value > 5") \
        .join(dim_df, "zone_id") \
        .withColumn("full_sensor_code", F.concat(F.lit("SNS-"), F.col("sensor_id"))) \
        .withColumn("value_bucket", (F.col("metric_value") / 10).cast("int"))

    silver_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("zone_id", "value_bucket") \
        .saveAsTable("sensor_cleaned")

    # ---------------------------
    # GOLD (Aggregations)
    # ---------------------------
    # Using 'sensor_stats_hourly'
    print("Writing: raw.sensor_stats_hourly (Gold)")

    base_df = spark.read.table("sensor_cleaned")

    agg1 = base_df.groupBy(
        F.window("ingest_ts", "1 hour"),
        "zone_id"
    ).agg(
        F.avg("metric_value").alias("avg_val"),
        F.max("metric_value").alias("max_val"),
        F.count("*").alias("cnt")
    )

    agg2 = agg1.groupBy("zone_id").agg(
        F.avg("avg_val").alias("zone_avg"),
        F.max("max_val").alias("zone_peak"),
        F.sum("cnt").alias("total_events")
    )

    gold_df = agg1.join(agg2, "zone_id") \
        .select(
            F.col("window.start").alias("start_time"),
            "zone_id",
            "avg_val",
            "max_val",
            "zone_avg",
            "zone_peak",
            "total_events"
        )

    gold_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("zone_id") \
        .saveAsTable("sensor_stats_hourly")

    # ---------------------------
    # EXTRA STRESS (Pivot)
    # ---------------------------
    print("Running extra stress query (Pivot)...")

    stress_df = spark.read.table("sensor_cleaned") \
        .groupBy("zone_id") \
        .pivot("value_bucket") \
        .agg(F.avg("metric_value"))

    stress_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("sensor_pivot_stress")

    print("\n--- CONCURRENT TEST COMPLETE ---")
    spark.sql("SHOW TABLES").show(truncate=False)

except Exception as e:
    print(f"Pipeline Failed: {e}")

finally:
    if 'spark' in locals():
        spark.stop()