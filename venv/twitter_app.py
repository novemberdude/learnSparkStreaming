# PROBLEM: khong pass duoc package kafka vao script, ne khong chay duoc Structure Streaming ket noi voi Kafka bang IDE
# --> phai dung spark-submit.

import json
from time import sleep
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from twython import Twython
import os

# setup arguments
import os
print("ok")
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64/'
print(os.environ.get("JAVA_HOME"))
os.environ.items()
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[2]'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local --total-executor 2 pyspark-shell'


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def ingest_data_by_spark(topic_name):
    # Spark Streaming
    spark = SparkSession.builder.master("local[1]").appName("Twitter sentiment analysis").getOrCreate()

    print(spark.version)

    df = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter_raw_data").load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # df.schema
    # # Generate running word count
    # data = df
    #
    # # Start running the query that prints the running counts to the console
    # query = data\
    #     .writeStream\
    #     .outputMode("append")\
    #     .format("console")\
    #     .start()
    #
    # query.awaitTermination()


if __name__ == '__main__':
    ingest_data_by_spark('twitter_raw_data')

















    # parsed_topic_name = 'twitter-data'
    # # Notify if a recipe has more than 200 calories
    # calories_threshold = 200
    #
    # consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
    #                          bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    # for msg in consumer:
    #     record = json.loads(msg.value)
    #     calories = int(record['calories'])
    #     title = record['title']
    #
    #     if calories > calories_threshold:
    #         print('Alert: {} calories count is {}'.format(title, calories))
    #     sleep(3)
    #
    # if consumer is not None:
    #     consumer.close()
