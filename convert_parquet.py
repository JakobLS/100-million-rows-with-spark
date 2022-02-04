from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, FloatType
import os


if __name__ == "__main__":

    # Input and output data paths
    data_csv = "cleaned-dataset-csv/cleaned_*.csv"
    movie_titles_path = "netflix-dataset/movie_titles.csv"
    output_data_parquet = "data-parquet"


    skip = False

    # If parquet file already exist, don't convert it. 
    if os.path.exists(output_data_parquet):
        skip = True


    if not skip:
        # Initiate SparkContext if currently not running
        try:
            sc = SparkContext(appName="CSV to Parquet")
            spark = SparkSession.builder.master("local").getOrCreate()
        except: 
            pass


    # Specify name and data type for each column and DataFrame
    schema_data = StructType([
        StructField("MovieID", IntegerType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Rating", FloatType(), True),
        StructField("RatingDate", DateType(), True)])
    schema_movies = StructType([
        StructField("MovieID", IntegerType(), True),
        StructField("YearOfRelease", IntegerType(), True)])


    # Load the datasets with the specified schemas
    if not skip:
        df = spark.read.csv("cleaned-dataset-csv/cleaned_*.csv", 
                            header=False, 
                            schema=schema_data)
        movie_titles = spark.read.csv(movie_titles_path, 
                                      schema=schema_movies)


    # Specify alias for the DataFrames to facilitate a join
    df = df.alias("df")
    movie_titles = movie_titles.alias("movie_titles")

    # Join the two DataFrames on MovieID. Select only columns of interest
    df = df.join(movie_titles,
                 df.MovieID == movie_titles.MovieID,
                 "left").select("df.*", "movie_titles.YearOfRelease")
    
    # Shuffle the data
    df = df.orderBy(rand())

    # Write to Parquet
    if not skip:
        df.write.parquet(output_data_parquet)
    
    # Stop SparkContext
    sc.stop()
