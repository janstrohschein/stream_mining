from configparser import ConfigParser
import sys
import json

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

    def on_data(self, data):
        tweet = self.read_json(data)
        if 'user' in self.config['Extract']['elements'] and 'user' in tweet:
            print(tweet['user'].get('name', 'No Name'), ':')

        if 'text' in self.config['Extract']['elements']:
            print(tweet.get('text', 'No text'))
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

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=l.config['Extract']['keywords'])
