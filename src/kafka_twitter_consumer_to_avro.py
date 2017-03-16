from kafka import KafkaConsumer

import avro.schema
import avro.io
import io
import sys
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

consumer = KafkaConsumer('tweets',\
                         group_id='twitter_to_avro',\
                         bootstrap_servers=['localhost:9092'])

class AvroWriter():

    def __init__(self):
        self.schema = avro.schema.Parse(open('tweet_full.avsc').read())
        self.avro_writer = DataFileWriter(open('tweet_full.avro', 'rb+'), DatumWriter(), codec="deflate")

my_avro = AvroWriter()

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(my_avro.schema)
    tweet = reader.read(decoder)
    my_avro.avro_writer.append(tweet)

    print(tweet)


