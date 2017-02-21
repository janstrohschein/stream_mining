from kafka import KafkaConsumer
import avro.schema
import avro.io
import io

consumer = KafkaConsumer('users',\
                         group_id='my_group',\
                         bootstrap_servers=['localhost:9092'])

schema = avro.schema.Parse(open('user.avsc').read())

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    user = reader.read(decoder)
    print(user)