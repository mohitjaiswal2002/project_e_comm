from pyspark.sql import SparkSession
from src.read_data import read_data

spark = SparkSession.builder.appName("Test").getOrCreate()

df = read_data(spark, "Projects\pyspark-ecommerce-pipeline\data\data.csv")  # ✅ correct relative path

df.show(10)