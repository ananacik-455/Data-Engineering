from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import when, input_file_name

import os


# Define the schema of your CSV data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), False),
    StructField("host_id", StringType(), False),
    StructField("host_name", StringType(), False),
    StructField("neighbourhood_group", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("room_type", StringType(), False),
    StructField("price", IntegerType(), False),
    StructField("minimum_nights", IntegerType(), False),
    StructField("number_of_reviews", IntegerType(), False),
    StructField("last_review", DateType(), False),
    StructField("reviews_per_month", DoubleType(), False),
    StructField("calculated_host_listings_count", IntegerType(), False),
    StructField("availability_365", IntegerType(), False)
])

# Paths
raw_folder_path = "../raw"
processed_data_path = "../processed"
processed_files_log = "../logs/processed_files_log.txt"

# Helper functions to manage processed files
def load_processed_files():
    if os.path.exists(processed_files_log):
        with open(processed_files_log, 'r') as f:
            return set(f.read().splitlines())
    return set()

def update_processed_files(file_name):
    with open(processed_files_log, "a") as file:
        file.write(f"{file_name}\n")


def process_streaming_data(spark):
    # Read streaming data
    streaming_df = spark.readStream \
                    .option("header", "true")\
                    .schema(schema) \
                    .csv(raw_folder_path)
    streaming_df = streaming_df.withColumn("file_path", input_file_name())
    
    # # This for detect and process only new files, but cant solve error
    # processed_files = load_processed_files()
    # filename = streaming_df.select("file_path").distinct().rdd.flatMap(lambda x: x).collect()

    # if filename not in processed_files:
    # Clean data
    cleaned_df = streaming_df \
                .filter(col("price") > 0) \
                .withColumn("last_review", to_date(col("last_review")))\
                .fillna({"reviews_per_month": 0})\
                .dropna(subset=["latitude", "longitude"])
    
    # Add transformation columns
    transformed_df = cleaned_df.withColumn(
        "price_category",
        when(col("price") < 100, "budget")
        .when((col("price") >= 100) & (col("price") <= 200), "mid-range")
        .otherwise("luxury")
    )
    
    transformed_df = transformed_df.withColumn(
        "price_per_review",
        col("price") / col("number_of_reviews")
    )
    try: 
        filename = transformed_df.select("file_path").distinct().rdd.flatMap(lambda x: x).collect()
        if filename:
            update_processed_files(filename)
    except:
        print('Now file currently to process')

    return transformed_df
    # else:
    #     return streaming_df

# Merge with existing data
def merge_data(new_df):
    # Load previously processed data if available
    if os.path.exists(processed_data_path):
        existing_df = spark.read.schema(new_df.schema).parquet(processed_data_path)
        merged_df = existing_df.unionByName(new_df)\
            .dropDuplicates(["host_id", "latitude", "longitude", "room_type", "price"])
    else:
        merged_df = new_df
    return merged_df

def save_data(merged_df):
    merged_df_repartitioned = merged_df.repartition("neighbourhood_group")
    merged_df_repartitioned.write \
        .mode("overwrite") \
        .partitionBy("neighbourhood_group") \
        .parquet(processed_data_path)

# SQL queries function
def perform_sql_queries(merged_df):
    merged_df.createOrReplaceTempView("airbnb_listings")

    listings_by_neigh_group = spark.sql("""
        SELECT neighbourhood_group, COUNT(*) as count
        FROM airbnb_listings
        GROUP BY neighbourhood_group
        ORDER BY count DESC
    """)
    
    top_10_expensive = spark.sql("""
        SELECT *
        FROM airbnb_listings
        ORDER BY price DESC
        LIMIT 10
    """)
    
    avg_price_by_room_type = spark.sql("""
        SELECT room_type, neighbourhood_group, AVG(price) as avg_price
        FROM airbnb_listings
        GROUP BY room_type, neighbourhood_group
    """)
    
    return listings_by_neigh_group, top_10_expensive, avg_price_by_room_type

# Data Quality Checks
def data_quality_checks(df):
    record_count = df.count()
    print(f"Number of records: {record_count}")

    null_checks = df.filter(
        col("price").isNull() |
        col("minimum_nights").isNull() |
        col("availability_365").isNull()
    )

    if null_checks.count() > 0:
        print("There are NULL values in critical columns.")
    else:
        print("No NULL values in critical columns.")

def process_and_save_batch(df_batch, batch_id):
    
    # Merge new data with existing data and save
    merged_df = merge_data(df_batch)

    # Perform SQL queries
    listings_by_neigh_group, top_10_expensive, avg_price_by_room_type = perform_sql_queries(merged_df)

    # Save results to CSV
    listings_by_neigh_group.write\
        .format('csv')\
        .mode("overwrite") \
        .save("../logs/listings_by_neigh_group.csv")
    top_10_expensive.write\
        .format('csv')\
        .mode("overwrite") \
        .save("../logs/top_10_expensive.csv")
    avg_price_by_room_type.write\
        .format('csv')\
        .mode("overwrite") \
        .save("../logs/avg_price_by_room_type.csv")
    
    # Show sql results
    listings_by_neigh_group.show(10)
    top_10_expensive.show(10)
    avg_price_by_room_type.show(10)

    # Perform data quality checks
    data_quality_checks(merged_df)

    # Save data
    save_data(merged_df)
    
# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYC Airbnb ETL") \
    .getOrCreate()

# Process streaming data
new_df = process_streaming_data(spark)


query = new_df.writeStream \
    .foreachBatch(process_and_save_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()


