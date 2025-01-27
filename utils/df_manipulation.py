from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

"""
This tool merges and removes duplicates from two Spark Dataframes.

- Inputs:
    * df1: first Dataframe
    * df2: second Dataframe

- Outputs:
    * result_df: merged and cleaned DataFrame
"""
def merge_clean_df(df1, df2):
    merged_df = df1.union(df2)

    merged_df = merged_df.withColumn("Date", col("Date").cast("timestamp"))
    window_spec = Window.partitionBy("id").orderBy(col("Date").desc())
    ranked_df = merged_df.withColumn("row_num", row_number().over(window_spec))
    result_df = ranked_df.filter(col("row_num") == 1).drop("row_num")

    return result_df