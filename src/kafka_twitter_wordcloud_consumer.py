from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
from datetime import datetime, timedelta
import re

consumer = KafkaConsumer('tweets',\
                         group_id='wordcloud',\
                         bootstrap_servers=['localhost:9092'])

schema = avro.schema.Parse(open('tweet_full.avsc').read())
tweet_list = []

from nltk.corpus import stopwords
stop = set(stopwords.words('english'))

def get_tweet(msg):
    bytes_reader = io.BytesIO(msg)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    tweet = reader.read(decoder)

    return tweet

for msg in consumer:
    tweet = get_tweet(msg.value)

    if 't.text' in tweet:
        tweet_text = re.sub('\s+', ' ', tweet['t.text'])
        print(tweet_text)

        tweet_list.append((datetime.now(), tweet_text.lower()))

        tod = datetime.now()
        d = timedelta(minutes = 60)
        a = tod - d
        if tweet_list[0][0] < a:
            print(tweet_list[0][0], '<', a)
            tweet_list.pop(0)

        # join tweets to a single string
        tweet_list_con = " ".join([ tweet[1] for tweet in tweet_list])

        # remove URLs, RTs, and twitter handles
        no_urls_no_tags = " ".join([word for word in tweet_list_con.split()
                                    if 'http' not in word
                                        and not word.startswith('@')
                                        and word != 'rt'
                                        and word not in stop
                                    ])
        from collections import Counter
        word_count = Counter(no_urls_no_tags.split())


        for word, count in word_count.most_common(10):
            print('%dx %s' % (count, word))
