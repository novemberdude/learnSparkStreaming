from twython import Twython
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import jsonlines as jsonl
import csv
import pandas as pd


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


# Create a class that inherits TwythonStreamer
def save_to_jsonl(tweet):
    with jsonl.open(r'data/saved_tweets.jsonl', mode='a') as writer:
        writer.write(tweet)


def get_twitter_data(conf_file, key_search, count_result):
    with open(conf_file, "r") as file:
        creds = json.load(file)

    python_tweets = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])
    data = python_tweets.search(q=key_search, result_type='Mixed', count=count_result)
    statuses = data['statuses']
    producer = connect_kafka_producer()
    for post in statuses:
        # publish_message(producer, 'twitter_raw_data', 'raw', str(post))
        save_to_jsonl(post)
        print(post)


if __name__ == '__main__':
    get_twitter_data('twitter_credentials.json', 'Vietnam', 100)








#
# # Create our query
# query = {'q': 'donal trump'}
# print(query)
# # Search tweets
# dict_ = {'user': [], 'date': [], 'text': [], 'favorite_count': []}
# for status in python_tweets.search(**query)['statuses']:
#     print(1)
#     print(status)
#     dict_['user'].append(status['user']['screen_name'])
#     dict_['date'].append(status['created_at'])
#     dict_['text'].append(status['text'])
#     dict_['favorite_count'].append(status['favorite_count'])
#
# # # Structure data in a pandas DataFrame for easier manipulation
# df = pd.DataFrame(dict_)
# # df.sort_values(by='favorite_count', inplace=True, ascending=False)
# print('start')
# print(df.head(5))
# print('end')
