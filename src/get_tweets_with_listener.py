from configparser import ConfigParser
import sys
import json
import csv
import codecs
import re

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def __init__(self):
        super(StdOutListener, self).__init__()
        self.config = {}
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

    def open_csv_writer(self):
        # Open/Create a file to append data
        #self.csvFile = open('result.csv', 'a')
        if 'csv' in self.config['Store']:
            self.csvFile = codecs.open(self.config['Store']['csv'][0], 'a', encoding="utf-8")
            print('Store data in ' + self.config['Store']['csv'][0])
        else:
            self.csvFile = codecs.open('result.csv', 'a', encoding="utf-8")
            print('Store data in standard file result.csv')
        #Use csv Writer
        self.csvWriter = csv.writer(self.csvFile)

    def on_data(self, data):
        tweet = self.read_json(data)
        tweet_extract = []
        if 'user' in self.config['Extract']['elements'] and 'user' in tweet:
            tweet_extract.append(tweet['user'].get('name', 'No Name') + ': ')

        if 'text' in self.config['Extract']['elements']:
            tweet_extract.append(tweet.get('text', 'No text'))

        tweet_extract = [re.sub('\s+',' ',item) for item in tweet_extract]

        if 'csv' in self.config['Store']:
            self.csvWriter.writerow(tweet_extract)
            # writes to file before self.csvFile.close()
            self.csvFile.flush()

        print(*tweet_extract)

        return True

    def on_error(self, status):
        print(status)

    def read_json(self, data):
        return json.loads(data, 'utf-8')


if __name__ == '__main__':
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(*l.config['Twitter']['consumer_key'], *l.config['Twitter']['consumer_secret'])
    auth.set_access_token(*l.config['Twitter']['access_token'], *l.config['Twitter']['access_token_secret'])
    stream = Stream(auth, l)

    # workaround for windows console missing utf-8
    sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer)

    l.open_csv_writer()
    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=l.config['Extract']['keywords'])
    l.csvFile.close()
