''' You need to ingest 50 TB of historical Parquet files from an S3 bucket into a Delta Lake table. The 
files are partitioned inconsistently, and some contain corrupt data. How would you handle this? '''

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from delta.tables import DeltaTable
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_spark_session():
    """Initialize Spark session with Delta Lake and S3 configurations."""
    return (SparkSession.builder
            .appName("ParquetToDeltaIngestion")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "1000")
            .config("spark.default.parallelism", "1000")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.sql.parquet.mergeSchema", "true")
            .getOrCreate())

def list_s3_files(bucket, prefix):
    """List all Parquet files in the S3 bucket."""
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            files.extend([obj['Key'] for obj in page['Contents'] if obj['Key'].endswith('.parquet')])
    
    return files

def process_batch(spark, s3_path, delta_table_path, batch_size=100):
    """Process Parquet files in batches to manage memory and handle corrupt data."""
    s3_client = boto3.client('s3')
    bucket = s3_path.split('/')[2]
    prefix = '/'.join(s3_path.split('/')[3:])
    
    files = list_s3_files(bucket, prefix)
    total_files = len(files)
    logger.info(f"Found {total_files} Parquet files to process.")
    
    for i in range(0, total_files, batch_size):
        batch_files = files[i:i + batch_size]
        batch_s3_paths = [f"s3://{bucket}/{file}" for file in batch_files]
        
        try:
            # Read Parquet files with schema merging and corruption handling
            df = (spark.read
                  .option("mergeSchema", "true")
                  .option("recursiveFileLookup", "true")
                  .option("ignoreCorruptFiles", "true")
                  .parquet(*batch_s3_paths)
                  .withColumn("source_file", input_file_name()))
            
            # Write to Delta Lake with schema evolution
            (df.write
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .save(delta_table_path))
            
            logger.info(f"Processed batch {i//batch_size + 1} with {len(batch_files)} files.")
            
        except Exception as e:
            logger.error(f"Error processing batch {i//batch_size + 1}: {str(e)}")
            # Log corrupt files to a separate table for analysis
            corrupt_df = spark.createDataFrame([(file, str(e)) for file in batch_files], ["file_path", "error"])
            corrupt_df.write.mode("append").saveAsTable("corrupt_files_log")
            
def optimize_delta_table(spark, delta_table_path):
    """Optimize the Delta table and enable auto-compaction."""
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    
    # Enable auto-optimize and auto-compaction
    spark.sql(f"""
        ALTER TABLE delta.`{delta_table_path}`
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    
    # Run optimize to compact small files
    delta_table.optimize().executeCompaction()
    logger.info("Delta table optimization completed.")
    
    # Vacuum old files
    delta_table.vacuum(168)  # Retain files for 7 days
    logger.info("Delta table vacuum completed.")

def main():
    s3_path = "s3://your-bucket/path/to/parquet"
    delta_table_path = "s3://your-bucket/delta_table"
    
    spark = initialize_spark_session()
    
    try:
        # Create log table for corrupt files
        spark.sql("""
            CREATE TABLE IF NOT EXISTS corrupt_files_log (
                file_path STRING,
                error STRING
            ) USING DELTA
        """)
        
        # Process files in batches
        process_batch(spark, s3_path, delta_table_path)
        
        # Optimize the Delta table
        optimize_delta_table(spark, delta_table_path)
        
        logger.info("Ingestion and optimization completed successfully.")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()