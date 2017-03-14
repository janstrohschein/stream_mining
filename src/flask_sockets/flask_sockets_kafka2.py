import time
import json
from flask import Flask, Response, redirect, request, url_for

from kafka import KafkaConsumer
import avro.schema
import avro.io
import io


app = Flask(__name__)

class TweetConsumer:

    def __init__(self):
        self.consumer = KafkaConsumer('wordcloud_output',\
                         group_id='flask_wordcloud',\
                         bootstrap_servers=['localhost:9092'])


@app.route('/')
def index():
    if request.headers.get('accept') == 'text/event-stream':

        def events():
            tweets = TweetConsumer()

            # iterates over the kafka consumer and yields the resulting word cloud
            for msg in tweets.consumer:
                # decode the current tweet with the avro in_schema
                data = msg.value.decode()
                word_cloud = json.loads(data, 'utf-8')

                # encodes the list to a json-object and yields it as result for this
                # iteration of the loop
                yield "data: %s \n\n" % (word_cloud)
                time.sleep(5)  # an artificial delay

        return Response(events(), content_type='text/event-stream')
    return redirect(url_for('static', filename='index.html'))

if __name__ == "__main__":
    app.run(host='localhost', port=23423)