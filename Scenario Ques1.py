'''Your organization receives streaming data from an IoT platform via Apache Kafka, with nested JSON 
payloads containing sensor readings. The data must be ingested into a Delta Lake table for real-time analytics, but 
the schema evolves frequently. How would you design a robust ingestion pipeline?'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_date
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("IoTKafkaToDeltaPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the initial schema for the nested JSON payload
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("readings", StructType([
        StructField("temp", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ]), True)
])

# Step 1: Read streaming data from Kafka
kafka_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka-broker:9092")
            .option("subscribe", "iot-sensor-topic")
            .option("startingOffsets", "latest")  # Start from latest offsets
            .load())

# Step 2: Parse the JSON payload
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string")

# Step 3: Parse JSON and handle malformed records
parsed_df = json_df.select(from_json(col("json_string"), schema).alias("data"))
valid_df = parsed_df.filter(col("data").isNotNull()).select("data.*")
error_df = parsed_df.filter(col("data").isNull()).select("json_string")

# Step 4: Add a date column for partitioning
valid_df = valid_df.withColumn("date", to_date(col("timestamp")))

# Step 5: Write valid data to Delta Lake table with schema evolution
output_table = "iot_schema.iot_sensor_data"
(valid_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")  # Enable schema evolution
        .option("checkpointLocation", "/delta/checkpoints/iot_sensor_data")
        .partitionBy("date")  # Partition by date for query performance
        .outputMode("append")
        .table(output_table))

# Step 6: Write malformed records to a separate Delta table for debugging
error_table = "iot_schema.iot_sensor_errors"
(error_df.writeStream
         .format("delta")
         .option("checkpointLocation", "/delta/checkpoints/iot_sensor_errors")
         .outputMode("append")
         .table(error_table))

# Step 7: Optimize the Delta table for real-time analytics (run periodically in a separate job)
# Example SQL command to optimize:
# spark.sql("OPTIMIZE iot_schema.iot_sensor_data ZORDER BY (device_id, timestamp)")

# Note: Keep the streaming query running
# query = valid_df.writeStream ... (already started above)
# query.awaitTermination()  # Uncomment to keep the job running in production