from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# Schema for weather data
weather_schema = StructType([
    StructField("temp", DoubleType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("temp_min", DoubleType(), True),
    StructField("temp_max", DoubleType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
])

# Initialize Spark session with Kafka & Postgres support
# Fix for Windows Docker Ivy path applied
spark = SparkSession.builder \
    .appName("WeatherConsumer") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0"
    ) \
    .config("spark.jars.ivy", "/tmp/.ivy") \
    .getOrCreate()

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value (bytes) to string
df_str = df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON into columns
df_parsed = df_str.select(from_json(col("json_str"), weather_schema).alias("data")).select("data.*")

# Postgres connection details
postgres_url = "jdbc:postgresql://postgres:5432/airflow"
postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Write streaming data to Postgres
query = df_parsed.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(
        url=postgres_url,
        table="weather",
        mode="append",
        properties=postgres_properties
    )) \
    .outputMode("update") \
    .start()

query.awaitTermination()
