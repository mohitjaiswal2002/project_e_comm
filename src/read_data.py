from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

def read_data(spark, path):
    logger.info(f"Reading data from {path}")

    df = (spark.read
          .schema(schema)
          .option("header", True)
          .csv(path))

    logger.info("Data read successfully")
    return df