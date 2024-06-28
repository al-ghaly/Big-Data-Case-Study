# Import needed packages
import pandas as pd
import os
import datetime
import subprocess
import hashlib


def create_hdfs_directory(*paths, logger):
    """
    Create directories in HDFS and log the results.

    This function attempts to create one or more directories in the Hadoop
    Distributed File System (HDFS) using the `hdfs dfs -mkdir -p` command.
    It logs the success or failure of each directory creation attempt.

    Parameters:
    ----------
    *paths : str
        One or more paths to be created in HDFS.
    logger : file-like object
        A logger object that supports the `write` method for logging events.

    Returns:
    -------
    None
    """
    # Loop over the paths to be created
    for path in paths:
        try:
            # Try to create a directory for the given path in HDFS
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", path], check=True)
            # Log the event
            message = f"Directory {path} created successfully in HDFS."
            logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
        except subprocess.CalledProcessError as e:
            message = f"An error happened trying to create {path} in HDFS {e}."
            logger.write(f"{str(datetime.datetime.now())}    ERROR    {message}\n")


def calculate_checksum(file_path, chunk_size=4096):
    """
    Calculate the MD5 checksum of a file.

    This function reads the file in chunks and calculates the MD5 hash value of its
    content. It handles large files efficiently by reading them in chunks.

    Parameters:
    ----------
    file_path : str
        The path to the file for which the checksum is to be calculated.
    chunk_size : int, optional
        The size of each chunk to read from the file. Default is 4096 bytes.

    Returns:
    -------
    str
        The hexadecimal representation of the MD5 checksum of the file.
    """
    # Open the file to calculate the hash value of its content
    with open(file_path, "rb") as f:
        # Set up the hash function
        hasher = hashlib.md5()
        while True:
            # Read the file chunk by chunk to handle large files efficiently
            chunk = f.read(chunk_size)
            # Break the loop when the end of the file is reached
            if not chunk:
                break
            # Calculate the Hash Value for this chunk
            hasher.update(chunk)
    # Return the hexadecimal representation of the hash
    return hasher.hexdigest()


def get_files(path, logger):
    """
    Retrieve unique files from a directory, excluding those in the 'archive' folder,
    and log events.

    This function walks through the given directory and calculates the checksum for
    each file to determine its uniqueness. It logs the process and any duplicate files
    found. Files inside the 'archive' folder or its subfolders are skipped.

    Parameters:
    ----------
    path : str
        The path to the directory containing the files.
    logger : file-like object
        A logger object that supports the `write` method for logging events.

    Returns:
    -------
    list
        A list of paths to unique files.

    Examples:
    --------
    >>> import logging
    >>> logger = logging.getLogger()
    >>> unique_files = get_files('/path/to/directory', logger)
    >>> print(unique_files)
    ['/path/to/directory/file1.txt', '/path/to/directory/file2.txt']
    """
    # A Dictionary to hold the hash values for each file
    file_hashes = {}
    # Loop over all the files in the given path
    for root, dirs, files in os.walk(path):
        # Skip the files inside archive folder
        if os.path.basename(root) == "archive":
            continue
        # Skip files inside subfolders of the archive folder
        elif os.path.basename(os.path.dirname(root)) == "archive":
            continue
        # Loop over the files
        for file in files:
            # Skip log file
            if file == "log_file.log":
                continue
            # Compose the absolute path
            file_path = os.path.join(root, file)
            # Check the uniqueness of the file using its check sum
            message = f"Calculating Check sum for {file_path}."
            logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
            checksum = calculate_checksum(file_path)
            message = f"Check Sum Calculated for {file_path}."
            logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
            # In case of a duplicate file
            if checksum in file_hashes:
                # Log the event in the log file
                message = f"{file} is duplicated!\nIt is the same as the file: {file_hashes[checksum]}"
                logger.write(f"{str(datetime.datetime.now())}    WARNING    {message}\n")
                continue
            # Update the hash values dictionary
            file_hashes[checksum] = file_path
    # Return list of unique files to upload to Data Lake
    return list(file_hashes.values())


def get_hdfs_path(local_source):
    """
    Determine the HDFS path for a given local file based on its category.

    This function constructs the HDFS path for a file by analyzing its name and the
    name of its parent directory. It categorizes the file into branches, sales agents,
    or sales transactions and creates a new file name based on these categories.

    Parameters:
    ----------
    local_source : str
        The local file path for which the HDFS path is to be determined.

    Returns:
    -------
    str or None
        The HDFS path for the file, or None if the file category is unknown.

    Examples:
    --------
    >>> hdfs_path = get_hdfs_path('/local/path/to/branch_data.csv')
    >>> print(hdfs_path)
    '/Spark_Project/data/Q_company/branches/parent_directory_branch_data.csv'
    """
    # Get the name of the file
    file_name = os.path.basename(local_source)
    # Get the name of the file group (The name of its parent directory)
    parent_directory = os.path.basename(os.path.dirname(local_source))
    # Compose the name of the file to be uploaded to HDFS
    hdfs_file_name = parent_directory + '_' + file_name

    # Categorize the file (Branches, Sales Agents, or Sales Transactions)
    if 'branch' in file_name.lower():
        hdfs_directory = "/Spark_Project/data/Q_company/branches"
    elif 'agent' in file_name.lower():
        hdfs_directory = "/Spark_Project/data/Q_company/sales_agents"
    elif 'transaction' in file_name.lower():
        hdfs_directory = "/Spark_Project/data/Q_company/sales_transactions"
    else:
        # Log unknown file!
        print("Unknown File!!")
        return
        # Return the HDFS path for this file
    return os.path.join(hdfs_directory, hdfs_file_name)


def upload_file(local_source, logger):
    """
    Upload a local file to HDFS and log the process.

    This function determines the HDFS path for a given local file, uploads the file
    to HDFS, and logs the process. If the file cannot be categorized or an error occurs
    during upload, appropriate messages are logged.

    Parameters:
    ----------
    local_source : str
        The path to the local file to be uploaded.
    logger : file-like object
        A logger object that supports the `write` method for logging events.

    Returns:
    -------
    bool
        True if the file is successfully uploaded, False otherwise.

    Examples:
    --------
    >>> import logging
    >>> logger = logging.getLogger()
    >>> success = upload_file('/local/path/to/file.txt', logger)
    >>> print(success)
    True
    """
    # Get the HDFS path for this file
    message = f"Composing HDFS Path for {local_source}"
    logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
    hdfs_destination = get_hdfs_path(local_source)
    # In case we could not categorize the file skip it
    if hdfs_destination is None:
        message = f"Unknown HDFS Destination for: {local_source}"
        logger.write(f"{str(datetime.datetime.now())}    ERROR    {message}\n")
        # Return False indicating an error
        return False
    try:
        # Upload the file to HDFS
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_source, hdfs_destination], check=True)
        message = f"Successfully copied {local_source} to {hdfs_destination} in HDFS."
        logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
        # Return True indicating a success
        return True
    except subprocess.CalledProcessError as e:
        # Return False indicating an error
        message = f"Failed to copy {local_source} to {hdfs_destination} in HDFS. Error: {e}"
        logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
        return False


def archive_file(path, logger):
    """
    Move a file to an archive folder and log the process.

    This function moves a given file to an archive folder, creating necessary
    directories if they do not exist, and logs the process. It logs success and
    failure events during the archiving process.

    Parameters:
    ----------
    path : str
        The path to the file to be archived.
    logger : file-like object
        A logger object that supports the `write` method for logging events.

    Examples:
    --------
    >>> import logging
    >>> logger = logging.getLogger()
    >>> archive_file('/local/path/to/file.txt', logger)
    """
    # Compose the path for the archive folder
    folder_path = os.path.join(local_path, 'archive')

    # Create the Archive folder if it does not exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        message = f"Archive Folder '{folder_path}' created successfully."
        logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")

    # Move the file to the Archive folder
    # Get the name of the file group
    parent_directory = os.path.basename(os.path.dirname(path))
    # Compose the path for this file group inside the archive directory
    archive_group = os.path.join(folder_path, parent_directory)
    # Create folder for this files group in the archive directory (If not exists)
    if not os.path.exists(archive_group):
        os.makedirs(archive_group)
        message = f"Folder '{archive_group}' created successfully inside the Archive Folder."
        logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
    try:
        # Move the file to the archive group
        subprocess.run(["mv", path, archive_group], check=True)
        message = f"Successfully moved {path} to Archive."
        logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
    except subprocess.CalledProcessError as e:
        message = f"Failed to move {path} to Archive. Error: {e}"
        logger.write(f"{str(datetime.datetime.now())}    ERROR    {message}\n")


def move_files(files, logger):
    """
    Add quality columns to each file, upload them to HDFS, and archive them.

    This function reads each file from the provided list, adds quality columns,
    uploads the modified file to HDFS, and archives the file if the upload is successful.
    All operations are logged using the provided logger.

    Parameters:
    ----------
    files : list of str
        A list of file paths to be processed.
    logger : file-like object
        A logger object that supports the `write` method for logging events.

    Examples:
    --------
    >>> import logging
    >>> logger = logging.getLogger()
    >>> files = ['/local/path/to/file1.csv', '/local/path/to/file2.csv']
    >>> move_files(files, logger)
    """
    # Loop over the files to be uploaded into HDFS
    for path in files:
        # Create a Pandas Dataframe to add some quality columns
        df = pd.read_csv(path)
        # Add log entries
        message = f"Adding quality columns to the file {path}"
        logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
        # Add the timestamp of the load time to the file
        current_timestamp = datetime.datetime.now()
        df['load_time'] = str(current_timestamp)
        # Add a column containing the load source
        df['source'] = 'Local File System'
        # Add a column containing the path of the file to be uploaded
        df['source_path'] = path
        # overwrite the original file with the new one
        df.to_csv(path, index=False)
        # Adding log entries
        message = f"Uploading file {path} to HDFS"
        logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
        # Upload the file into HDFS
        upload_status = upload_file(path, logger)
        # In case that the file is successfully uploaded archive it
        if upload_status:
            message = f"Archiving file: {path}"
            logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
            archive_file(path, logger)


def clean(path, logger):
    """
    Remove empty folders within a specified directory and log the process.

    This function traverses the specified directory and removes any empty folders,
    logging each removal using the provided logger.

    Parameters:
    ----------
    path : str
        The path to the directory to be cleaned.
    logger : file-like object
        A logger object that supports the `write` method for logging events.

    Examples:
    --------
    >>> import logging
    >>> logger = logging.getLogger()
    >>> clean('/local/path/to/clean', logger)
    """
    # Loop through all files and folders in the given directory
    for root, dirs, files in os.walk(path, topdown=False):
        # Loop through the directories
        for dir_name in dirs:
            # Compose the path for this directory
            dir_path = os.path.join(root, dir_name)
            # Check if the directory is empty
            if not os.listdir(dir_path):
                message = f"Deleting empty folder: {dir_path}"
                logger.write(f"{str(datetime.datetime.now())}    INFO    {message}\n")
                # Remove the empty directory
                os.rmdir(dir_path)


def open_log_file(file_path):
    """
    Open a log file in append mode, creating it if it doesn't exist.

    This function opens the specified log file in append mode. If the file does
    not exist, it is created. A message indicating the log file has been opened
    is written to the file.

    Parameters:
    ----------
    file_path : str
        The path to the log file to be opened or created.

    Returns:
    -------
    file object
        The file object for the opened log file.

    """
    # Open the log file in append mode
    if os.path.exists(file_path):
        mode = 'a'
    else:
        mode = 'w'
    log_file = open(file_path, mode)
    log_file.write(f"{str(datetime.datetime.now())} INFO Log file Opened.\n")
    # Return a file object
    return log_file


def automate():
    # Define the path for the log file
    log_file_path = "/data/Spark_project/log_file.log"
    # Open the log file for write
    log_file = open_log_file(log_file_path)
    # Create the needed directories in HDFS
    create_hdfs_directory("/Spark_Project/data/Q_company/branches",
                          "/Spark_Project/data/Q_company/sales_agents",
                          "/Spark_Project/data/Q_company/sales_transactions",
                          logger=log_file)

    # Specify the files to add to the Data Lake
    files_to_upload = get_files(local_path, logger=log_file)
    print(files_to_upload)
    # add the files to HDFS
    move_files(files_to_upload, logger=log_file)
    # Remove empty folders
    clean(local_path, logger=log_file)
    # Close the log file
    log_file.close()


if __name__ == "__main__":
    # Define the path for the file groups (LFS)
    local_path = "/data/Spark_project"
    automate()
    