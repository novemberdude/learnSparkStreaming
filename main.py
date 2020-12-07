# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from pyspark.sql import SparkSession


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

    spark = SparkSession.builder.master("local").appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()


    activityQuery = activityCounts.writeStream.queryName("activity_counts") \
        .format("memory").outputMode("complete") \
        .start()

    print(activityCounts)
    print(activityQuery)
    
    activityQuery.awaitTermination()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
