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


    stop = set(stopwords.words('english'))

    def get_tweet(self, msg):
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

            for msg in tweets.consumer:

                tweet = tweets.get_tweet(msg.value)

                if 't.text' in tweet:
                    tweet_text = re.sub('\s+', ' ', tweet['t.text'])
                    print(tweet_text)

                    tweets.tweet_list.append((datetime.now(), tweet_text.lower()))

                    tod = datetime.now()
                    d = timedelta(minutes = 60)
                    a = tod - d
                    if tweets.tweet_list[0][0] < a:
                        print(tweets.tweet_list[0][0], '<', a)
                        tweets.tweet_list.pop(0)

                    # join tweets to a single string
                    tweet_list_con = " ".join([ tweet[1] for tweet in tweets.tweet_list])

                    # remove URLs, RTs, and twitter handles
                    no_urls_no_tags = " ".join([word for word in tweet_list_con.split()
                                                if 'http' not in word
                                                    and not word.startswith('@')
                                                    and word != 'rt'
                                                    and word not in tweets.stop
                                                ])

                    word_count = Counter(no_urls_no_tags.split())

                    out_string = ""
                    out_list = []
                    for word, count in word_count.most_common(15):
                        out_string += str(count) + "x " + word + " "
                        out_list.append((word, count))
                        print('%dx %s' % (count, word))

                    out_json = json.dumps(out_list, 'utf-8')
                    yield "data: %s \n\n" % (out_json)

                    time.sleep(1.5)  # an artificial delay


            # for i, c in enumerate(itertools.cycle('\|/-')):
            #     yield "data: %s %d\n\n" % (c, i)
            #     time.sleep(.1)  # an artificial delay
        return Response(events(), content_type='text/event-stream')
    return redirect(url_for('static', filename='index.html'))

if __name__ == "__main__":
    app.run(host='localhost', port=23423)