from pyspark.sql import SparkSession

from utils.load_data import load_csv_data
from utils.df_manipulation import merge_clean_df

def main():
    spark = SparkSession.builder.appName("CleanDB").getOrCreate()

    df1 = load_csv_data(spark, "data/db1.csv")
    df2 = load_csv_data(spark, "data/db2.csv")
    
    merged_df = merge_clean_df(df1, df2)
    merged_df.show()

    spark.stop()

if __name__ == "__main__":
    main()