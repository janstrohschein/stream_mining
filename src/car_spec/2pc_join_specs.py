from src.car_spec.KafkaPC import KafkaPC


class JoinSpecsPC(KafkaPC):
    def __init__(self, in_topic, in_group, in_schema_file, out_topic, out_schema_file):
        super().__init__(in_topic, in_group, in_schema_file, out_topic, out_schema_file)

        self.Spec = {}
        for i in range(1,30000):
            self.Spec[i] = [str(i) + "S1", str(i) + "S2", str(i) + "S3",
                            str(i) + "S4", str(i) + "S5", str(i) + "S6"]


new_pc = JoinSpecsPC('carlist', 'join_specs', 'carlist.avsc', 'speclist', 'speclist.avsc')

for msg in new_pc.consumer:
    carlist = new_pc.decode_msg(msg)
    new_spec = new_pc.Spec[carlist['VIN']]

    speclistdata = {
        "VIN": carlist['VIN'],
        "Line": carlist['Line'],
        "Date": carlist['Date'],
        "Time": carlist['Time'],
    }

    for item in new_spec:
        print(carlist['VIN'], carlist['Line'], item, carlist['Date'], carlist['Time'])

        speclistdata['Spec'] = item
        new_pc.send_msg(speclistdata)

