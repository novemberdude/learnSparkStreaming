from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Twitter sentiment analysis").getOrCreate()