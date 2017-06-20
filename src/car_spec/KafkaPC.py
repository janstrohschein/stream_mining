import sys
import io

from kafka import KafkaProducer, KafkaConsumer

from avro.io import BinaryEncoder, DatumWriter
import avro.schema


class KafkaPC():
    def __init__(self, in_topic, in_group, in_schema_file, out_topic, out_schema_file):
        super(KafkaPC, self).__init__()

        self.out_topic = out_topic
        self.out_schema_file = out_schema_file
        self.out_schema = avro.schema.Parse(open(self.out_schema_file).read())
        # if no broker_id is provided the KafkaProducer will connect
        # on localhost:9092
        self.producer = KafkaProducer()

        self.in_topic = in_topic
        self.in_schema_file = in_schema_file
        self.in_schema = avro.schema.Parse(open(self.in_schema_file).read())
        self.consumer = KafkaConsumer(in_topic, \
                                      group_id=in_group, \
                                      bootstrap_servers=['localhost:9092'])

    def decode_msg(self, msg):
        bytes_reader = io.BytesIO(msg.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.in_schema)
        return reader.read(decoder)

    def __encode(self, data):
        raw_bytes = None
        try:
            writer = DatumWriter(self.out_schema)
            bytes_writer = io.BytesIO()
            encoder = BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            raw_bytes = bytes_writer.getvalue()
        except:
            print("Error encoding data", sys.exc_info())
        return raw_bytes

    def send_msg(self, data):

        # encode the CarList with the specified Avro in_schema
        raw_bytes = self.__encode(data)

        # publish the message if encoding was successful
        if raw_bytes is not None:
            try:
                self.producer.send(self.out_topic, raw_bytes)
            except:
                print("Error sending message to kafka")
