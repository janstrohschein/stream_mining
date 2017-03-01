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

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    tweet = reader.read(decoder)

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


        # import numpy as np
        # from sklearn.feature_extraction.text import CountVectorizer
        #
        # vectorizer = CountVectorizer(analyzer = "word",
        #                              tokenizer = None,
        #                              preprocessor = None,
        #                              stop_words = None,
        #                              min_df = 0,
        #                              max_features = 50)
        #
        # text = ["Hello I am going to I with hello am"]
        #
        # # Count
        # train_data_features = vectorizer.fit_transform(no_urls_no_tags)
        # vocab = vectorizer.get_feature_names()
        #
        # # Sum up the counts of each vocabulary word
        # dist = np.sum(train_data_features.toarray(), axis=0)
        #
        # # For each, print the vocabulary word and the number of times it
        # # appears in the training set
        # for tag, count in zip(vocab, dist):
        #     print(count, tag)
