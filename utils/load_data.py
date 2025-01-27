"""
This tool loads a Spark Dataframe from a .csv file.

- Inputs:
    * spark: Spark active session
    * file: path to .csv file

- Outputs:
    * df: Spark Dataframe
"""
def load_csv_data(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)