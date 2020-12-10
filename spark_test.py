import os
import findspark
findspark.init('/home/hungpm/tools/spark-2.4.7-bin-hadoop2.7/')
from pyspark.sql import SparkSession

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7"
spark = SparkSession \
    .builder \
    .appName("twitter-sentiment-analysis3") \
    .getOrCreate()
