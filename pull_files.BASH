# !/bin/bash

# Source directory
SOURCE_DIR="="/Source_To_File_Groups"

# Destination directory
DEST_DIR="/data/Spark_Project/"

# Log file path
LOG_FILE="/data/Spark_Project/log_file.log"

# Function to move directories
move_directories()
{
    echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Starting to move directories from $SOURCE_DIR to $DEST_DIR" >> "$LOG_FILE"

mv "$SOURCE_DIR"/* "$DEST_DIR"

echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Moved directory $dir_name to $DEST_DIR" >> "$LOG_FILE"


echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: Finished moving directories" >> "$LOG_FILE"
}

# Run the function
move_directories