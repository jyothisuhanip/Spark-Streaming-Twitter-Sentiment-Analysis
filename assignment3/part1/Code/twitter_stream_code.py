from kafka import KafkaProducer
import json
import tweepy
from tweepy import StreamingClient
import re
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
BEARER_TOKEN = ""

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def clean_tweet(tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
class IDPrinter(tweepy.StreamingClient):
    
    def on_data(self, data):
        # try:
        tweet_data = json.loads(data)
        tweet = tweet_data["data"]["text"]
        if 'RT' not in tweet[:5]:
            tweet = tweet_data["data"]["text"]
            tweet = clean_tweet(tweet)
            future = producer.send(config['arguments']['topic'], tweet.encode('utf-8'))
            print(tweet)
        return True

streaming_client = IDPrinter(BEARER_TOKEN)
streaming_client.add_rules(tweepy.StreamRule(config['arguments']['search']))   
streaming_client.filter()




