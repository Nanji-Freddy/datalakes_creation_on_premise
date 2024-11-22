from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit,trim,to_date, year, explode, struct,to_timestamp,lower ,regexp_extract, to_timestamp, upper,date_format
from pyspark.sql.functions import sum as spark_sum, avg as spark_avg
from pyspark.sql.types import IntegerType, StringType, FloatType,StructType, StructField,MapType
from sqlalchemy import create_engine, text
import sqlalchemy as db
import pandas as pd
import os
from mage_demo.utils.spark_session_factory import get_spark_session

# Function to stop any existing Spark session
def stop_existing_spark_session():
    try:
        existing_spark = SparkSession.builder.getOrCreate()
        if existing_spark:
            existing_spark.stop()
    except Exception as e:
        print(f"No existing Spark session to stop: {e}")

stop_existing_spark_session()

MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')

iceberg_spark_session = get_spark_session(
    "iceberg",
    app_name="MageSparkSession",
    warehouse_path="s3a://iceberg-demo-bucket/warehouse",
    s3_endpoint="http://minio:9000",
    s3_access_key=MINIO_ACCESS_KEY,
    s3_secret_key=MINIO_SECRET_KEY
)
client = Minio(
    "minio:9000",
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

minio_bucket = "iceberg-demo-bucket"
found = client.bucket_exists(minio_bucket)
if not found:
    client.make_bucket(minio_bucket)
    
    
@custom
def iceberg_table_write(*args, **kwargs):
    data_folder = "mage_demo/data"  # Adjust this path according to your directory structure
    for filename in os.listdir(data_folder):
        file_path = os.path.join(data_folder, filename)
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        table_name = f"local.iceberg_demo.{base_name}"

        if filename.endswith(".csv"):
            
            
            # Read the CSV file into a Spark DataFrame
            df = iceberg_spark_session.spark.read.csv(file_path, header=True, inferSchema=True)
            # Write into Minio using Iceberg
            
            
            if base_name == 'inconsistent_ad_campaign_data_with_cars':
                print('process inconsistent_ad_campaign_data_with_cars')
                # Drop rows with nulls in critical fields: `ad_id`, `campaignID`, `event_type`, and `timestamp`
                df = df.na.drop(subset=["ad_id", "campaignID", "event_type", "timestamp"])

                # Standardize timestamp format
                # Convert timestamp to a consistent datetime format, handling both date formats
                df = df.withColumn("timestamp", 
                    when(df["timestamp"].rlike(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}"), to_timestamp("timestamp", "MM/dd/yyyy HH:mm"))
                    .when(df["timestamp"].rlike(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"), to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                    .otherwise(to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
                )

                # Cast `cost` to a float type and replace non-numeric values with 0
                df = df.withColumn("cost", when(col("cost").cast("float").isNotNull(), col("cost").cast("float")).otherwise(0.0))

                # Remove duplicates based on unique identifiers
                df = df.dropDuplicates(["ad_id", "timestamp"])
                
                # Standardize `device_type` and `event_type` to uppercase
                df = df.withColumn("device_type", upper(col("device_type")))
                df = df.withColumn("event_type", upper(col("event_type")))

                # Derive metrics based on `event_type`
                # Add columns for impression, click, and conversion indicators
                df = df.withColumn("impression", when(col("event_type") == "IMPRESSION", lit(1)).otherwise(lit(0)))
                df = df.withColumn("click", when(col("event_type") == "CLICK", lit(1)).otherwise(lit(0)))
                df = df.withColumn("conversion", when(col("event_type") == "CONVERSION", lit(1)).otherwise(lit(0)))

            
            
            if df.count() == 0:
                print(f"No data to write for table {table_name}. Skipping.")
                continue

            try:
                df.writeTo(table_name).createOrReplace()
                print(f"Data written to table: {table_name}")
            except Exception as e:
                print(f"Error writing to table {table_name}: {e}")

        elif filename.endswith(".db"):
            
            engine = db.create_engine(f'sqlite:///{file_path}')
            connection = engine.connect()
            sql_table_name = "e_commerce_cars_transactions"  # Replace with the actual table name
            sql_query = text(f"SELECT * FROM {sql_table_name}")
            pandas_df = pd.read_sql(sql_query, connection)
            
            df = iceberg_spark_session.spark.createDataFrame(pandas_df)
            
            
            if base_name == 'car_ecommerece':
                df = df.na.fill({
                    "country": "Unknown",         # Default value for missing country
                    "zip_code": "00000",          # Default value for missing zip code
                    "payment_method": "unknown",  # Default value for missing payment method
                    "transaction_status": "unknown",
                    "amount": 0.0                 # Default value for amount if missing
                })

                # Enforce Data Types
                # Convert 'amount' to FloatType and 'car_model_year' to IntegerType
                df = df.withColumn("amount", col("amount").cast(FloatType()))
                df = df.withColumn("car_model_year", col("car_model_year").cast(IntegerType()))
                # Remove Duplicates based on Unique Identifier
                # Assuming 'transaction_id' is a unique identifier for transactions
                df = df.dropDuplicates(["transaction_id"])

                # Range Check: Set reasonable bounds for numeric columns
                # Set bounds for 'amount' (e.g., 0 to 100 million) and validate 'car_model_year'
                df = df.withColumn(
                    "amount",
                    when((col("amount") >= 0) & (col("amount") <= 1e8), col("amount")).otherwise(lit(None))
                )
                df = df.withColumn(
                    "car_model_year",
                    when((col("car_model_year") >= 1886) & (col("car_model_year") <= 2024), col("car_model_year")).otherwise(lit(None))
                )

                # Validate 'car_model_year': Convert to IntegerType and check range (e.g., between 1886 and 2024)
                df = df.withColumn("car_model_year", col("car_model_year").cast(IntegerType()))
                df = df.withColumn(
                    "car_model_year",
                    when((col("car_model_year") >= 1886) & (col("car_model_year") <= 2024), col("car_model_year"))
                    .otherwise(lit(None))
                )

                # Convert 'transaction_date' to a consistent date format and filter out invalid dates
                # Assuming 'transaction_date' is initially in 'MM/dd/yyyy' format
                df = df.withColumn("transaction_date", to_date(col("transaction_date"), "M/d/yyyy"))

                # Standardize String Columns (e.g., trim whitespace and convert to title case)
                df = df.withColumn("last_name", trim(col("last_name")))
                df = df.withColumn("first_name", trim(col("first_name")))
                df = df.withColumn("car_name", trim(col("car_name")))
                df = df.withColumn("car_model", trim(col("car_model")))
                df = df.withColumn("country", trim(col("country")))
                df = df.withColumn("car_name", trim(col("car_name")).alias("car_name"))
                df = df.withColumn("car_model", trim(col("car_model")).alias("car_model"))
                
            if df.count() == 0:
                print(f"No data to write for table {table_name}. Skipping.")
                continue

            try:
                df.writeTo(table_name).createOrReplace()
                print(f"Data written to table: {table_name}")
            except Exception as e:
                print(f"Error writing to table {table_name}: {e}")
        elif filename.endswith(".txt"):
            
            df = iceberg_spark_session.spark.read.text(file_path)
            
            
            
            if base_name == 'fictive_web_server_logs':
                print('process fictive_web_server_logs')
                log_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - IP: (\S+) - Action: (\S+) - Endpoint: (\S+) - Status: (\d{3}) - User-Agent: "(.*)"'

                # Extract fields using regex and create structured DataFrame
                df = df.select(
                    regexp_extract("value", log_pattern, 1).alias("timestamp"),
                    regexp_extract("value", log_pattern, 2).alias("ip"),
                    regexp_extract("value", log_pattern, 3).alias("action"),
                    regexp_extract("value", log_pattern, 4).alias("endpoint"),
                    regexp_extract("value", log_pattern, 5).cast("integer").alias("status"),
                    regexp_extract("value", log_pattern, 6).alias("user_agent")
                )


                # Define additional regex patterns for parsing the `user_agent`
                platform_pattern = r'^([^ ]+)'  # e.g., "Mozilla/5.0"
                os_device_pattern = r'\(([^)]+)\)'  # e.g., "Linux; Android 10; SM-G973F"
                browser_engine_pattern = r'AppleWebKit/\d+\.\d+'  # e.g., "AppleWebKit/537.36"
                browser_pattern = r'(Chrome|Safari|Firefox|Edge)/[\d.]+'  # e.g., "Chrome/87.0.4280.141"
                
                
                # Extract components from `user_agent`
                df = df.withColumn("platform", regexp_extract("user_agent", platform_pattern, 1))
                df = df.withColumn("os_device", regexp_extract("user_agent", os_device_pattern, 1))
                df = df.withColumn("browser_engine", regexp_extract("user_agent", browser_engine_pattern, 0))
                df = df.withColumn("browser", regexp_extract("user_agent", browser_pattern, 0))
                
                # Filter out malformed records where critical fields are null
                df = df.filter(df["timestamp"].isNotNull() & df["ip"].isNotNull() & df["status"].isNotNull())
                # Convert `timestamp` to a proper datetime format
                df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

                # Standardize `action` to uppercase
                df = df.withColumn("action", upper(col("action")))
                
                df = df.withColumn("date", date_format("timestamp", "yyyy-MM-dd"))
                df = df.withColumn("hour", date_format("timestamp", "HH"))

                # Categorize status codes
                df = df.withColumn(
                    "status_category",
                    when((col("status") >= 200) & (col("status") < 300), "Success")
                    .when((col("status") >= 400) & (col("status") < 500), "Client Error")
                    .when((col("status") >= 500) & (col("status") < 600), "Server Error")
                    .otherwise("Other")
                )
                
            if df.count() == 0:
                print(f"No data to write for table {table_name}. Skipping.")
                continue

            try:
                df.writeTo(table_name).createOrReplace()
                print(f"Data written to table: {table_name}")
            except Exception as e:
                print(f"Error writing to table {table_name}: {e}")
            
        elif filename.endswith(".json"):
            file_path = os.path.join(data_folder, filename)
            
            df = iceberg_spark_session.spark.read.option("multiline", "true").json(file_path)
            
            
            
            if base_name == 'fictive_social_media_data':
                print('process fictive_social_media_data')
                # Flatten the `engagement_metrics` struct into individual columns
                df = df.withColumn("likes", col("engagement_metrics.likes"))
                df = df.withColumn("comments", col("engagement_metrics.comments"))
                df = df.withColumn("shares", col("engagement_metrics.shares"))
                df = df.drop("engagement_metrics")  # Drop the original struct column to avoid further issues

                # Fill missing demographic values
                df = df.na.fill({
                    "age_group": "unknown",
                    "gender": "unknown",
                    "location": "unknown",
                    "car_make": "unknown",
                    "car_model": "unknown"
                })

                # Fill missing engagement metrics with defaults
                df = df.na.fill({
                    "likes": 0,
                    "comments": 0,
                    "shares": 0
                })

                # Convert timestamp to a consistent datetime format
                df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))

                # Standardize demographic and car model information
                df = df.withColumn("age_group", trim(col("age_group")))
                df = df.withColumn("gender", trim(col("gender")))
                df = df.withColumn("location", trim(col("location")))
                df = df.withColumn("car_make", trim(col("car_make")))
                df = df.withColumn("car_model", trim(col("car_model")))
                df = df.withColumn("post_content", trim(col("post_content")))

                # Validate action types
                VALID_ACTIONS = ["viewed_post", "liked_post", "commented_on_post", "shared_post", "bookmarked_post"]
                df = df.withColumn("action", lower(trim(col("action"))))
                df = df.withColumn(
                    "action",
                    when(col("action").isin(VALID_ACTIONS), col("action")).otherwise("unknown")
                )

                # Set thresholds for engagement metrics
                LIKES_THRESHOLD = 1000
                COMMENTS_THRESHOLD = 500
                SHARES_THRESHOLD = 200

                df = df.withColumn(
                    "likes",
                    when(col("likes") <= LIKES_THRESHOLD, col("likes")).otherwise(lit(None))
                )
                df = df.withColumn(
                    "comments",
                    when(col("comments") <= COMMENTS_THRESHOLD, col("comments")).otherwise(lit(None))
                )
                df = df.withColumn(
                    "shares",
                    when(col("shares") <= SHARES_THRESHOLD, col("shares")).otherwise(lit(None))
                )
                
            if df.count() == 0:
                print(f"No data to write for table {table_name}. Skipping.")
                continue

            try:
                df.writeTo(table_name).createOrReplace()
                print(f"Data written to table: {table_name}")
            except Exception as e:
                print(f"Error writing to table {table_name}: {e}")
            
        else:
            print(f"Unsupported file format: {filename}")
                
        
    return "Iceberg csv tables created successfully"
    
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'