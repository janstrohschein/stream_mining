from kafka import KafkaConsumer
import avro.schema
import avro.io
import io

consumer = KafkaConsumer('tweets',\
                         group_id='my_group',\
                         bootstrap_servers=['localhost:9092'])

schema = avro.schema.Parse(open('tweet.avsc').read())

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    tweet = reader.read(decoder)
    print(tweet)