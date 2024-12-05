import os
from pyspark.sql import SparkSession, DataFrame
import json
import argparse

def set_bcg_directory():
    """
    Sets the current working directory to the 'BCG' folder located 
    in the user's home directory, if it exists. Provides feedback 
    if the directory is missing.
    """
    # Retrieving the user's home directory path
    home_directory = os.path.expanduser("~")
    
    # Constructing the path to the 'BCG' directory
    bcg_directory = os.path.join(home_directory, 'BCG')
    
    # Validating if the 'BCG' directory exists and is a valid directory
    if os.path.isdir(bcg_directory):
        os.chdir(bcg_directory)
        print(f"Current working directory successfully set to: {os.getcwd()}")
    else:
        print(f"Error: The directory '{bcg_directory}' does not exist.")


def parse_arguments():
    """Used for parsing the command-line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=str, default="config.json")
    args = parser.parse_args()
    return args.config
    
def load_csv_data_to_df(spark: SparkSession, file_path: str, header: bool = True, inferSchema: str = True) -> DataFrame:
    """
    Load a CSV file into a PySpark DataFrame.
    """
    return spark.read.option("inferSchema", inferSchema).csv(file_path, header=header)

def read_json(file_path: str) -> dict:
    """
    Read a config file in JSON format.
    """
    with open(file_path, "r") as f:
        return json.load(f)


def save_dataframe_to_file(df: DataFrame, output_path: str, file_format:str):
    """
    Save DataFrame to a file in the specified format (e.g., CSV, Parquet).
    """
    # Validating the write format
    supported_formats = ["csv", "parquet", "json", "orc", "avro"]
    if file_format not in supported_formats:
        raise ValueError(f"Unsupported write format: {file_format}. Supported formats are: {', '.join(supported_formats)}")
    if not output_path:
        raise ValueError("Output path cannot be empty.")
    try:
        df.repartition(1).write.format(file_format).mode("overwrite").option("header", "true").save(output_path)
    except Exception as e:
        raise Exception(f"Error saving DataFrame: {str(e)}")




