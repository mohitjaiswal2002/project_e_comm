from pyspark.sql import SparkSession
import logging

from src.read_data import read_data
from src.transform import clean_data, transform_data
from src.write_data import write_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():

    spark = SparkSession.builder \
        .appName("Ecommerce Pipeline") \
        .getOrCreate()

    # Read
    df = read_data(spark, "Projects\pyspark-ecommerce-pipeline\data\data.csv")

    # Clean
    df_clean = clean_data(df)

    # Transform
    df_final, country_df, customer_df = transform_data(df_clean)

    # Write Parquet (for project)
    write_data(df_final, "Projects\pyspark-ecommerce-pipeline\output_parquet", "parquet")

    # Stop Spark
    spark.stop()


if __name__ == "__main__":
    main() 