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
class StdOutListener(StreamListener):
    def __init__(self):
        super(StdOutListener, self).__init__()
        self.config = {}
        self.num_tweets = 0
        self.read_config()

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

    def open_writers(self):
        if 'csv' in self.config['Store']:
            try:
                self.open_csv_writer()
            except:
                print('Could not open CSV Writer')

        if 'avro_schema' in self.config['Store'] and \
                        'avro_file' in self.config['Store']:
            try:
                self.open_avro_writer()
            except:
                print('Could not open Avro Writer')

    def open_csv_writer(self):
        #set path from ini file
        csv_path = 'result.csv'
        if 'csv' in self.config['Store']:
            csv_path = self.config['Store']['csv'][0]

        # Open/Create a file to append data
        if 'csv_append' in self.config['Store'] and self.config['Store']['csv_append'][0] == 'True':
            self.csvFile = codecs.open(csv_path, 'a', encoding="utf-8")
        else:
            self.csvFile = codecs.open(csv_path, 'w', encoding="utf-8")

        print('Store data in ' + csv_path)

        # Use csv Writer
        self.csvWriter = csv.writer(self.csvFile)

    def open_avro_writer(self):
        #set path from ini file
        schema_path = 'tweet.avsc'
        file_path = 'tweet.avro'

        if 'avro_schema' in self.config['Store']:
            schema_path = self.config['Store']['avro_schema'][0]
        if 'avro_file' in self.config['Store']:
            file_path = self.config['Store']['avro_file'][0]

        if 'avro_append' in self.config['Store'] and self.config['Store']['avro_append'][0] == 'True' \
                and os.path.isfile(file_path) is True:
            self.avro_writer = DataFileWriter(open(file_path, 'rb+'), DatumWriter(), codec="deflate")
        else:
            self.avro_schema = avro.schema.Parse(open(schema_path).read())
            self.avro_writer = DataFileWriter(open(file_path, 'wb'), DatumWriter(), self.avro_schema, codec="deflate")

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

        if 'csv' in self.config['Store']:
            self.csvWriter.writerow(tweet_extract)

        self.avro_writer.append({"name": tweet_extract[0], "text": tweet_extract[1]})

        print(*tweet_extract)
        self.num_tweets += 1

        return True

    def on_error(self, status):
        print(status)

    def read_json(self, data):
        return json.loads(data, 'utf-8')


if __name__ == '__main__':
    # This handles Twitter authentication and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(*l.config['Twitter']['consumer_key'], *l.config['Twitter']['consumer_secret'])
    auth.set_access_token(*l.config['Twitter']['access_token'], *l.config['Twitter']['access_token_secret'])
    stream = Stream(auth, l)

    # adds utf-8 support to windows console
    sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer)

    try:
        reader = DataFileReader(open("tweet.avro", 'rb'), DatumReader())
        for tweet in reader:
            print(tweet)
        reader.close()
    except:
        print('Reader error:', sys.exc_info())

    l.open_writers()
    # uses the keywords defined in the ini-file
    stream.filter(track=l.config['Extract']['keywords'])

    l.csvFile.close()
    l.avro_writer.close()
