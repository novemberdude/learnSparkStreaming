import json
import sys

import happybase
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils

if sys.version_info > (3, 0):
    py_version = 3
else:
    py_version = 2


def save_to_hbase(sc, t, rdd, family_table="heatmap_data"):
    """
    Save data as batch to hbase table
    Args:
        batch: batch object which created from heatmap table
        update_batch: batch object which created from heatmap_update table
        t (int): time in milliseconds
        rdd (RDD): Spark RDD

    Returns: None

    """

    hbase_host = sc.getConf().get("spark.hbase.ncov_hbase_host", "10.193.79.159")
    hbase_port = int(sc.getConf().get("spark.hbase.ncov_hbase_port", "9090"))
    window = int(sc.getConf().get("spark.streaming.ncov_window", "60"))

    tb_heatmap = sc.getConf().get("spark.hbase.ncov_tb_heatmap", "heatmap")
    tb_heatmap_update = sc.getConf().get("spark.hbase.ncov_tb_heatmap_update", "heatmap_update")
    hbase_batch_size = int(sc.getConf().get("spark.hbase.ncov_hbase_batch_size", "1000"))

    hconn = happybase.Connection(host=hbase_host, port=hbase_port, protocol='binary', timeout=window * 2 * 1000,
                                 transport='buffered')
    # protocol='compact', transport='framed')

    htable = hconn.table(tb_heatmap)
    batch = htable.batch(batch_size=hbase_batch_size)

    update_hconn = happybase.Connection(host=hbase_host, port=hbase_port, protocol='binary', timeout=window * 2 * 1000,
                                        transport='buffered')
    # protocol='compact', transport='framed')
    update_htable = update_hconn.table(tb_heatmap_update)
    update_batch = update_htable.batch(batch_size=hbase_batch_size)

    if not rdd.isEmpty():
        batch.put("%sUPDATE_TIME", {"%s:time" % family_table: str(t)})
        update_batch.put("UPDATE_TIME", {"%s:time" % family_table: str(t)})
        result = rdd.collect()
        for val in result:
            batch.put("%s_%s" % (str(val[0]), str(t)), {"%s:count" % family_table: str(val[1])})
            update_batch.put("%s" % (str(val[0])), {"%s:count" % family_table: str(val[1])})
        try:
            batch.send()
        except Exception as inst:
            print(inst)
        try:
            update_batch.send()
            pass
        except Exception as inst:
            print(inst)

    hconn.close()
    update_hconn.close()


def stream(sc):
    """
    Analyze a stream from kafka and save result to hbase
    Args:
        batch (HBaseBatch): hbase heatmap table batch object
        update_batch (HBaseBatch): hbase heatmap_update table batch object

    Returns: None

    """
    window = int(sc.getConf().get("spark.streaming.ncov_window", "60"))
    topic = sc.getConf().get("spark.kafka.ncov_topic", "topic3")
    # kafka_group = sc.getConf().get("spark.kafka.ncov_group", "quy")
    family_table = sc.getConf().get("spark.hbase.ncov_family_table", "heatmap_data")
    brokers = sc.getConf().get("spark.kafka.ncov_brokers", "10.193.79.172:9092")

    ssc = StreamingContext(sc, window)

    field_format = ['COUNTY_GID', 'DEVICE_ID', 'ID', 'LABEL', 'LAT', 'LOCALITY_GID', 'LON', 'PHONE_NUMBER', 'REGION_GID',
                    'TIMESTAMP']
    delimiter = "\t\t\t"
    field_format_dict = dict(zip(field_format, range(len(field_format))))
    brokers = brokers
    topics = topic.split(",")

    # kstream = KafkaUtils.createDirectStream(ssc, topics, {"metadata.broker.list": brokers})
    kstream = None
    if py_version == 3:
        dstream = kstream.map(lambda x: x[1])
    else:
        dstream = kstream.map(lambda x: x[1].encode('utf-8', 'ignore'))
    # Production
    flat_dstream = dstream.map(lambda x: json.loads(x)).map(lambda x: [x['COUNTY_GID'.lower()], x[
        'DEVICE_ID'.lower()], x['ID'.lower()], x['LABEL'.lower()], x['LAT'.lower()], x['LOCALITY_GID'.lower()], x[
            'LON'.lower()], x['PHONE_NUMBER'.lower()], x['REGION_GID'.lower()], x['TIMESTAMP'.lower()]])
    # # Test
    # flat_dstream = dstream.map(lambda x: str(x).split(delimiter))
    locality_dstream = flat_dstream.map(
        lambda x: ((x[field_format_dict['DEVICE_ID']], x[field_format_dict['LOCALITY_GID']]), 1)).reduceByKey(
        lambda x, y: 1).map(lambda x: (x[0][1], x[1])).reduceByKey(
        lambda x, y: x + y).map(lambda x: ("L_" + str(x[0]), x[1]))
    county_dstream = flat_dstream.map(
        lambda x: ((x[field_format_dict['DEVICE_ID']], x[field_format_dict['COUNTY_GID']]), 1)).reduceByKey(
        lambda x, y: 1).map(lambda x: (x[0][1], x[1])).reduceByKey(
        lambda x, y: x + y).map(lambda x: ("C_" + str(x[0]), x[1]))
    region_dstream = flat_dstream.map(
        lambda x: ((x[field_format_dict['DEVICE_ID']], x[field_format_dict['REGION_GID']]), 1)).reduceByKey(
        lambda x, y: 1).map(lambda x: (x[0][1], x[1])).reduceByKey(
        lambda x, y: x + y).map(lambda x: ("R_" + str(x[0]), x[1]))

    all_dstream = locality_dstream.union(county_dstream).uIntnion(region_dstream)
    try:
        all_dstream.foreachRDD(
            lambda t, rdd: save_to_hbase(sc, t, rdd, family_table=family_table))
    except Exception as inst:
        print(inst)

    ssc.start()
    try:
        ssc.awaitTermination()
    except:
        ssc.stop(False, False)
        raise Exception


def run(sc):
    stream(sc)


if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName('PythonStreamingKafkaWordCount')
    sc = SparkContext(conf=conf)
    run(sc)
    # while True:
    #     try:
    #         run(sc)
    #     except:
    #         time.sleep(5)
    #         pass