#!/usr/bin/env python
import itertools
import time
from collections import Counter
from flask import Flask, Response, redirect, request, url_for

from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
from datetime import datetime, timedelta
import re
from nltk.corpus import stopwords
import json

app = Flask(__name__)

class TweetConsumer:

    def __init__(self):
        self.consumer = KafkaConsumer('tweets',\
                         group_id='wordcloud',\
                         bootstrap_servers=['localhost:9092'])
        self.schema_path = "C:\\Users\\jan\\Documents\\GitHub\\stream_mining\\src\\tweet_full.avsc"
        self.schema = avro.schema.Parse(open(self.schema_path).read())
        self.tweet_list = []

    # uses the english and spanish stopword-lists from nltk and some additional
    # twitter/web words
    stop = set(stopwords.words('english'))
    stop.update(stopwords.words('spanish'))
    stop.update(['rt', '&', '-', '|', ':', '&amp'])

    def get_tweet(self, msg):
        """
        Decodes the binary tweet object with the according avro in_schema

        :param msg: the tweet to decode
        :return:
        """
        bytes_reader = io.BytesIO(msg)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        tweet = reader.read(decoder)

        return tweet


@app.route('/')
def index():
    if request.headers.get('accept') == 'text/event-stream':

        def events():
            tweets = TweetConsumer()

            # iterates over the kafka consumer and yields the resulting word cloud
            for msg in tweets.consumer:
                # decode the current tweet with the avro in_schema
                tweet = tweets.get_tweet(msg.value)

                #if there is a text-element in the new tweet object
                if 't.text' in tweet:
                    # remove stopwords, URLs, RTs, and twitter handles
                    tweet_text = re.sub('\s+', ' ', tweet['t.text'])
                    tweet_text = tweet_text.lower()
                    tweet_text_clean = ""
                    for word in re.split(r'[,;\'\"`´’ ]+', tweet_text):
                        if 'http' not in word and \
                            not word.startswith('@') and \
                            not word.startswith('.') and \
                            word not in tweets.stop:
                            tweet_text_clean += ' ' + word

                    # stores tweets with a timestamp so old tweets can be removed
                    tweets.tweet_list.append((datetime.now(), tweet_text_clean))

                    # removes tweets that are older than 60 minutes
                    tod = datetime.now()
                    d = timedelta(minutes = 60)
                    a = tod - d
                    if tweets.tweet_list[0][0] < a:
                        print(tweets.tweet_list[0][0], '<', a)
                        tweets.tweet_list.pop(0)

                    # join tweets to a single string so the Counter can work on it afterwards
                    tweet_list_con = " ".join([ tweet[1] for tweet in tweets.tweet_list])

                    # creates counts of the 15 most common words in no_urls_no_tags
                    # and stores them in out_list as tuples of (word, count)
                    word_count = Counter(tweet_list_con.split())
                    out_list = [(word, count) for word, count in word_count.most_common(15)]

                    # encodes the list to a json-object and yields it as result for this
                    # iteration of the loop
                    out_json = json.dumps(out_list, 'utf-8')
                    yield "data: %s \n\n" % (out_json)
                    time.sleep(.5)  # an artificial delay

        return Response(events(), content_type='text/event-stream')
    return redirect(url_for('static', filename='index.html'))

if __name__ == "__main__":
    app.run(host='localhost', port=23423)