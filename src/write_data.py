import logging

logger = logging.getLogger(__name__)

def write_data(df, path, file_format="parquet"):

    logger.info(f"Writing data to {path} in {file_format}")

    # Force local filesystem (important fix)
    path = "file:///" + path.replace("\\", "/")

    if file_format == "csv":
        (df.write
         .mode("overwrite")
         .option("header", True)
         .csv(path))

    elif file_format == "parquet":
        (df.write
         .mode("overwrite")
         .parquet(path))

    else:
        raise ValueError("Unsupported format")

    logger.info("Write completed")