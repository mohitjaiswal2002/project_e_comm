import logging
import os
import shutil

logger = logging.getLogger(__name__)

def write_data(df, path, file_format="parquet"):

    logger.info(f"Writing data to {path} in {file_format}")

    # delete existing folder
    if os.path.exists(path):
        shutil.rmtree(path)

    if file_format == "csv":
        (df.coalesce(1)
         .write
         .mode("overwrite")
         .option("header", True)
         .csv(path))

    elif file_format == "parquet":
         (df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(path))

    else:
        raise ValueError("Unsupported format")

    logger.info("Write completed")