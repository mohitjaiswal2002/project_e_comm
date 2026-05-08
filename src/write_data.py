import logging

logger = logging.getLogger(__name__)

def write_data(df, path, file_format="parquet"):
    logger.info(f"Writing data to {path} in {file_format}")

    writer = df.coalesce(1).write.mode("overwrite")

    if file_format == "csv":
        writer.option("header", True).csv(path)
    elif file_format == "parquet":
        writer.parquet(path)
    else:
        raise ValueError(f"Unsupported format: {file_format}")

    logger.info("Write completed")
