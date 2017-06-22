import sys
import io
import random
import datetime
import time

from kafka import KafkaProducer

from avro.io import BinaryEncoder, DatumWriter
import avro.schema

class CarListP():

    def __init__(self):
        super(CarListP, self).__init__()
        # if no broker_id is provided the KafkaProducer will connect
        # on localhost:9092
        self.producer = KafkaProducer()
        self.topic = 'carlist'

        self.schema_file = 'carlist.avsc'
        self.schema = avro.schema.Parse(open(self.schema_file).read())

    def encode(self, data):
        raw_bytes = None
        try:
            writer = DatumWriter(self.schema)
            bytes_writer = io.BytesIO()
            encoder = BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            raw_bytes = bytes_writer.getvalue()
        except:
            print("Error encoding data", sys.exc_info())
        return raw_bytes

    def send_msg(self, data):

        # encode the CarList with the specified Avro in_schema
        raw_bytes = self.encode(data)

        # publish the message if encoding was successful
        if raw_bytes is not None:
            try:
                self.producer.send(self.topic, raw_bytes)
            except:
                print("Error sending message to kafka")



# generate data all at once
dataset = [ (i, random.randrange(1,3), str(datetime.date.today())) for i in range(1,30000)]

new_producer = CarListP()

for data in dataset:

    # conforming data to the avro schema
    carlistdata = {
        "VIN": data[0],
        "Line": data[1],
        "Date": data[2],
        "Time": str(datetime.datetime.now().time())
    }

    print(carlistdata)
    new_producer.send_msg(carlistdata)

    #time.sleep(.02)
