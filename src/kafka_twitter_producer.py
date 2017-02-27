import sys
import json
import io
from configparser import ConfigParser

from kafka import SimpleProducer, SimpleClient

from avro.io import BinaryEncoder, DatumWriter
import avro.schema

# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream



# This is a basic listener that just prints received tweets to stdout.
class KafkaListener(StreamListener):
    def __init__(self):
        super(KafkaListener, self).__init__()
        self.config = {}
        self.read_config()

        # configuration file can contain a max number, listener stops then
        self.num_tweets = 0

        self.kafka = SimpleClient('localhost:9092')
        self.producer = SimpleProducer(self.kafka)

        self.topic = 'tweets'

        self.schema_file = 'tweet_full.avsc'
        if 'avro_schema' in self.config['Store']:
            self.schema_file = self.config['Store']['avro_schema'][0]
        self.schema = avro.schema.Parse(open(self.schema_file).read())

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

        # assign all field values to the corresponding schema names
        tweet_data = {
            "t.id": tweet.get('id'),
            "t.created_at": tweet.get('created_at'),
            "t.timestamp_ms": tweet.get('timestamp_ms'),
            "t.text": tweet.get('text'),
            "t.favourite_count": tweet.get('favourite_count'),
            "t.favorited": tweet.get('favorited'),
            "t.in_reply_to_screen_name": tweet.get('in_reply_to_screen_name'),
            "t.in_reply_to_status_id": tweet.get('in_reply_to_status_id'),
            "t.in_reply_to_user_id": tweet.get('in_reply_to_user_id'),
            "t.is_quote_status": tweet.get('is_quote_status'),
            "t.retweeted": tweet.get('retweeted'),

            "t.u.id": tweet['user'].get('id'),
            "t.u.name": tweet['user'].get('name'),
            "t.u.screen_name": tweet['user'].get('screen_name'),
            "t.u.description": tweet['user'].get('description'),
            "t.u.lang": tweet['user'].get('lang'),
            "t.u.location": tweet['user'].get('location'),
            "t.u.statuses_count": tweet['user'].get('statuses_count'),
            "t.u.followers_count": tweet['user'].get('followers_count'),
            "t.u.favourites_count": tweet['user'].get('favourites_count'),
            "t.u.friends_count": tweet['user'].get('friends_count'),
            "t.u.created_at": tweet['user'].get('created_at'),
        }
        if 'quoted_status' in tweet:
            tweet_data["q.id"] = tweet['quoted_status'].get('id')
            tweet_data["q.created_at"] = tweet['quoted_status'].get('created_at')
            tweet_data["q.text"] = tweet['quoted_status'].get('text')
            tweet_data["q.in_reply_to_screen_name"] = tweet['quoted_status'].get('in_reply_to_screen_name')
            tweet_data["q.in_reply_to_status_id"] = tweet['quoted_status'].get('in_reply_to_status_id')
            tweet_data["q.in_reply_to_user_id"] = tweet['quoted_status'].get('in_reply_to_user_id')
            tweet_data["q.u.id"] = tweet['quoted_status']['user'].get('id')
            tweet_data["q.u.name"] = tweet['quoted_status']['user'].get('name')
            tweet_data["q.u.screen_name"] = tweet['quoted_status']['user'].get('screen_name')
            tweet_data["q.u.description"] = tweet['quoted_status']['user'].get('description')
            tweet_data["q.u.lang"] = tweet['quoted_status']['user'].get('lang')
            tweet_data["q.u.location"] = tweet['quoted_status']['user'].get('location')
            tweet_data["q.u.statuses_count"] = tweet['quoted_status']['user'].get('statuses_count')
            tweet_data["q.u.followers_count"] = tweet['quoted_status']['user'].get('followers_count')
            tweet_data["q.u.favourites_count"] = tweet['quoted_status']['user'].get('favourites_count')
            tweet_data["q.u.friends_count"] = tweet['quoted_status']['user'].get('friends_count')
            tweet_data["q.u.created_at"] = tweet['quoted_status']['user'].get('created_at')

        if 'retweeted_status' in tweet:
            tweet_data["r.id"] = tweet['retweeted_status'].get('id')
            tweet_data["r.created_at"] = tweet['retweeted_status'].get('created_at')
            tweet_data["r.text"] = tweet['retweeted_status'].get('text')
            tweet_data["r.in_reply_to_screen_name"] = tweet['retweeted_status'].get('in_reply_to_screen_name')
            tweet_data["r.in_reply_to_status_id"] = tweet['retweeted_status'].get('in_reply_to_status_id')
            tweet_data["r.in_reply_to_user_id"] = tweet['retweeted_status'].get('in_reply_to_user_id')
            tweet_data["r.u.id"] = tweet['retweeted_status']['user'].get('id')
            tweet_data["r.u.name"] = tweet['retweeted_status']['user'].get('name')
            tweet_data["r.u.screen_name"] = tweet['retweeted_status']['user'].get('screen_name')
            tweet_data["r.u.description"] = tweet['retweeted_status']['user'].get('description')
            tweet_data["r.u.lang"] = tweet['retweeted_status']['user'].get('lang')
            tweet_data["r.u.location"] = tweet['retweeted_status']['user'].get('location')
            tweet_data["r.u.statuses_count"] = tweet['retweeted_status']['user'].get('statuses_count')
            tweet_data["r.u.followers_count"] = tweet['retweeted_status']['user'].get('followers_count')
            tweet_data["r.u.favourites_count"] = tweet['retweeted_status']['user'].get('favourites_count')
            tweet_data["r.u.friends_count"] = tweet['retweeted_status']['user'].get('friends_count')
            tweet_data["r.u.created_at"] = tweet['retweeted_status']['user'].get('created_at')

        # discards all fields where "get" returned None
        tweet_data = {k: v for k, v in tweet_data.items() if v!=None}

        # encode the tweet with the specified Avro schema
        raw_bytes = self.encode(tweet_data)

        # publish the message if encoding was successful
        if raw_bytes is not None:
            self.send(self.topic, raw_bytes)

        self.num_tweets += 1

        return True

    def on_error(self, status):
        print(status)

    def read_json(self, data):
        return json.loads(data, 'utf-8')

    def encode(self, data):
        raw_bytes = None
        try:
            writer = DatumWriter(self.schema)
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

# uses the keywords defined in the ini-file
stream.filter(track=l.config['Extract']['keywords'])