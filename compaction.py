import logging
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import subprocess
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting Data Compaction Job")

# Create SparkSession
spark = SparkSession.builder \
    .appName("DataCompactionJob") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

logger.info("Spark session created")

# Get the current date and hour
current_time = datetime.now()
current_date = current_time.strftime("%Y-%m-%d")
current_hour = (current_time - timedelta(hours=1)).strftime("%H")  # Process data for the previous hour

# Define the input and output path
input_path = f"/user/itversity/prj/date={current_date}/hour={current_hour}"
output_path = f"/user/itversity/prj/date={current_date}/hour={current_hour}_compacted"

# Check if the input path exists
check_path_command = f"hdfs dfs -test -d {input_path}"
if subprocess.run(check_path_command, shell=True).returncode != 0:
    logger.error(f"Input path {input_path} does not exist. Exiting.")
    sys.exit(1)

logger.info(f"Reading data from {input_path}")

# Read the data from the specified path
df = spark.read.parquet(input_path)

logger.info("Data read successfully")

# Repartition the data to control the number of output files
num_partitions = 1  # Adjust based on your needs
compacted_df = df.repartition(num_partitions)

logger.info(f"Repartitioned the data into {num_partitions} partitions")

# Write the data back to the same folder (or a different folder)
compacted_df.write.mode("overwrite").parquet(output_path)

logger.info(f"Compacted data written to {output_path}")

# Delete the old folder
delete_command = f"hdfs dfs -rm -r {input_path}"
logger.info(f"Deleting old data from {input_path}")
subprocess.run(delete_command, shell=True, check=True)

# Rename the compacted folder to the original folder name
rename_command = f"hdfs dfs -mv {output_path} {input_path}"
logger.info(f"Renaming {output_path} to {input_path}")
subprocess.run(rename_command, shell=True, check=True)

logger.info("Data compaction job completed successfully")

spark.stop()
