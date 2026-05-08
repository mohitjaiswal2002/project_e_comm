import logging
from pyspark.sql import SparkSession
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

    df = read_data(spark, "data/data.csv")
    df_clean = clean_data(df)
    df_final, country_df, customer_df = transform_data(df_clean)
    write_data(df_final, "output_parquet", "parquet")

    spark.stop()

if __name__ == "__main__":
    main()