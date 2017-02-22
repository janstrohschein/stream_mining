from kafka import SimpleProducer, SimpleClient
import avro.schema
import io
from avro.io import DatumWriter, BinaryEncoder
import sys
from configparser import ConfigParser
import sys
import json
import csv
import codecs
import re
import os

# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# This is a basic listener that just prints received tweets to stdout.
class KafkaListener(StreamListener):
    def __init__(self):
        super(KafkaListener, self).__init__()
        self.config = {}
        self.num_tweets = 0
        self.read_config()
        self.kafka = SimpleClient('localhost:9092')
        self.producer = SimpleProducer(self.kafka)
        self.topic = 'tweets'
        self.schema = 'tweet.avsc'

    def read_config(self):
        config = ConfigParser()
        config.read("C:/Users/Jan/Documents/GitHub/stream_mining/src/twitter.ini", encoding='utf-8')

        try:
            sections = config.sections()
            if len(sections) == 0:
                sys.exit()
            for section in sections:

                self.config[section] = {}
                for option in config.options(section):
                    self.config[section][option] = config.get(section, option).split(',')
        except:
            print("The config file path is not valid")
            sys.exit()


    def on_data(self, data):
        if 'num_tweets' in self.config['Extract'] and \
                        self.num_tweets == int(self.config['Extract']['num_tweets'][0]):
            return False

        tweet = self.read_json(data)
        tweet_extract = []
        if 'user' in self.config['Extract']['elements'] and 'user' in tweet:
            tweet_extract.append(tweet['user'].get('name') + ': ')
        else:
            tweet_extract.append('No Name: ')

        if 'text' in self.config['Extract']['elements'] and 'text' in tweet:
            tweet_extract.append(tweet.get('text'))
        else:
            tweet_extract.append('No Text')

        tweet_extract = [re.sub('\s+', ' ', item) for item in tweet_extract]


        tweet_data = {
            "name": tweet_extract[0],
            "text": tweet_extract[1]
        }

        raw_bytes = self.encode(self.schema, tweet_data)
        if raw_bytes is not None:
            self.send(self.topic, raw_bytes)

        print(*tweet_extract)
        self.num_tweets += 1

        return True

    def on_error(self, status):
        print(status)

    def read_json(self, data):
        return json.loads(data, 'utf-8')

    def encode(self, schema_file, data):
        raw_bytes = None
        try:
            schema = avro.schema.Parse(open(schema_file).read())
            writer = DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            raw_bytes = bytes_writer.getvalue()
        except:
            print("Error encode data", sys.exc_info())
        return raw_bytes

    def send(self, topic, raw_bytes):
        try:
            self.producer.send_messages(topic, raw_bytes)
        except:
            print("Error send message to kafka")


# This handles Twitter authentication and the connection to Twitter Streaming API
l = KafkaListener()
auth = OAuthHandler(*l.config['Twitter']['consumer_key'], *l.config['Twitter']['consumer_secret'])
auth.set_access_token(*l.config['Twitter']['access_token'], *l.config['Twitter']['access_token_secret'])
stream = Stream(auth, l)

# adds utf-8 support to windows console
sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer)

# uses the keywords defined in the ini-file
stream.filter(track=l.config['Extract']['keywords'])