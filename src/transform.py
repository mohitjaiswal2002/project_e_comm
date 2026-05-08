import logging
from pyspark.sql.functions import col, to_timestamp, expr

logger = logging.getLogger(__name__)

def clean_data(df):

    logger.info("Starting cleaning process")

    # Cast types
    df = (df
          .withColumn("Quantity", col("Quantity").cast("int"))
          .withColumn("UnitPrice", col("UnitPrice").cast("double"))
         .withColumn("InvoiceDate",
          expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')"))
         )

    # Remove nulls
    df = df.filter(
        col("Quantity").isNotNull() &
        col("UnitPrice").isNotNull() &
        col("InvoiceDate").isNotNull()
    )

    # Remove duplicates
    df = df.dropDuplicates()

    # Remove invalid values
    df = df.filter(
        (col("Quantity") > 0) &
        (col("UnitPrice") > 0)
    )

    logger.info("Cleaning completed")

    return df


def transform_data(df):

    logger.info("Starting transformation")

    # Revenue column
    df = df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))

    # Top countries
    country_df = (df
                  .groupBy("Country")
                  .sum("Revenue")
                  .withColumnRenamed("sum(Revenue)", "TotalRevenue"))

    # Top customers
    customer_df = (df
                   .groupBy("CustomerID")
                   .sum("Revenue")
                   .withColumnRenamed("sum(Revenue)", "TotalRevenue"))

    logger.info("Transformation completed")

    return df, country_df, customer_df