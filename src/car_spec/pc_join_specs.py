from kafka import KafkaConsumer, KafkaProducer
import avro.io
import io
from .KafkaPC import KafkaPC

class JoinSpecsPC(KafkaPC):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.consumer = KafkaConsumer('carlist',\
                                 group_id='join_specs',\
                                 bootstrap_servers=['localhost:9092'])

        self.producer = KafkaProducer()
        self.topic = 'speclist'
        self.schema = avro.schema.Parse(open('carlist.avsc').read())

        self.Spec = {}
        for i in range(30000):
            self.Spec[i] = [str(i) + "S1", str(i) + "S2", str(i) + "S3",
                            str(i) + "S4", str(i) + "S5", str(i) + "S6"]

    def decode(self, msg):
        bytes_reader = io.BytesIO(msg.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        return reader.read(decoder)


new_pc = JoinSpecsPC()

for msg in new_pc.consumer:
    carlist = new_pc.decode(msg)
    new_spec = new_pc.Spec[carlist['VIN']]
    for item in new_spec:
        print(carlist['VIN'], carlist['Line'], item, carlist['Date'], carlist['Time'])