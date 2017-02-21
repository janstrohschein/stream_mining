from kafka import SimpleProducer, SimpleClient
import avro.schema
import io
from avro.io import DatumWriter, BinaryEncoder
import sys

kafka = SimpleClient('localhost:9092')
producer = SimpleProducer(kafka)

def encode(schema_file, data):
    raw_bytes = None
    try:
        schema = avro.schema.Parse(open(schema_file).read())
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        raw_bytes = bytes_writer.getvalue()
    except:
        print("Error encode data", sys.exc_info())
    return raw_bytes

def send(topic, raw_bytes):
    try:
        producer.send_messages(topic, raw_bytes)
    except:
        print("Error send message to kafka")


topic = 'users'
user_data = {
    "id": 1,
    "name": "Jan Strohschein",
    "address": "Kölner Straße 57, 50226 Frechen"
}

raw_bytes = encode('user.avsc', user_data)
if raw_bytes is not None:
    send(topic, raw_bytes)