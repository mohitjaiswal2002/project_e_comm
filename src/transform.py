import logging
from pyspark.sql.functions import col, expr, sum, round

logger = logging.getLogger(__name__)


def clean_data(df):
    logger.info("Starting cleaning process")

    df = (df
          .withColumn("Quantity", col("Quantity").cast("int"))
          .withColumn("UnitPrice", col("UnitPrice").cast("double"))
          .withColumn("InvoiceDate", expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')"))
          .filter(col("Quantity").isNotNull() &
                  col("UnitPrice").isNotNull() &
                  col("InvoiceDate").isNotNull())
          .filter((col("Quantity") > 0) & (col("UnitPrice") > 0))
          .dropDuplicates()
    )

    logger.info("Cleaning completed")
    return df


def transform_data(df):
    logger.info("Starting transformation")

    # Cache since df is used in 3 derived dataframes
    df = df.withColumn("Revenue", round(col("Quantity") * col("UnitPrice"), 2))

    country_df = (df
                  .groupBy("Country")
                  .agg(round(sum("Revenue"), 2).alias("TotalRevenue"))
                  .orderBy(col("TotalRevenue").desc())
    )

    customer_df = (df
                   .filter(col("CustomerID").isNotNull())
                   .groupBy("CustomerID")
                   .agg(round(sum("Revenue"), 2).alias("TotalRevenue"))
                   .orderBy(col("TotalRevenue").desc())
    )

    logger.info("Transformation completed")
    return df, country_df, customer_df