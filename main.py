from pyspark.sql import SparkSession
import time

# Replace with your actual Spark Connect Server IP or hostname
# If running on the same machine as Docker, use 'localhost'
remote_url = "sc://100.84.28.115:15002"

print(f"--- Attempting to connect to {remote_url} ---")

try:
    # 1. Initialize the Remote Spark Session
    spark = SparkSession.builder.remote(remote_url).getOrCreate()
    
    print("--- Connection Successful! ---")

    # 2. Create some test data
    data = [("ETL_Test_1", 100), ("ETL_Test_2", 200), ("ETL_Test_3", 300)]
    columns = ["Task_Name", "Value"]
    
    df = spark.createDataFrame(data, schema=columns)

    # 3. Perform a transformation (This triggers the Executors)
    print("--- Processing Data on Cluster... ---")
    result = df.withColumn("Value_Plus_Tax", df["Value"] * 1.1)
    
    # 4. Show the result
    result.show()

    print("--- Test Complete. Check your Spark UI at http://localhost:4040 ---")

except Exception as e:
    print(f"--- Connection Failed! ---")
    print(f"Error: {e}")

finally:
    # Always stop the session to free up resources immediately
    if 'spark' in locals():
        spark.stop()