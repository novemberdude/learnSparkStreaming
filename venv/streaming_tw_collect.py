from twython import TwythonStreamer
import csv


# Filter out unwanted data
def process_tweet(tweet):
    d = {'hashtags': [hashtag['text'] for hashtag in tweet['entities']['hashtags']], 'text': tweet['text'],
         'user': tweet['user']['screen_name'], 'user_loc': tweet['user']['location']}
    return d


# Create a class that inherits TwythonStreamer
def save_to_csv(tweet):
    with open(r'saved_tweets.csv', 'a') as file:
        writer = csv.writer(file)
        writer.writerow(tweet)


class MyStreamer(TwythonStreamer):

    # Received data
    def on_success(self, data):
        # Only collect tweets in English
        if data['lang'] == 'en':
            tweet_data = process_tweet(data)
            self.save_to_csv(tweet_data)

    # Problem with the API
    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()

    # Save each tweet to csv file
