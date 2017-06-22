import random
from src.car_spec.KafkaPC import KafkaPC


class JoinLabourPC(KafkaPC):
    def __init__(self, in_topic, in_group, in_schema_file, out_topic, out_schema_file):
        super().__init__(in_topic, in_group, in_schema_file, out_topic, out_schema_file)

        self.Labour = {}
        for i in range(1,30000):
            for j in range(1,7):
                labourtimes = [("WS" + str(ws), random.randrange(1,20)) for ws in range(1,21)]

                self.Labour[str(i)+"S"+str(j)] = labourtimes


new_pc = JoinLabourPC('speclist', 'join_labour', 'speclist.avsc', 'labourlist', 'labourlist.avsc')

for msg in new_pc.consumer:
    speclist = new_pc.decode_msg(msg)
    new_labour = new_pc.Labour[speclist['Spec']]

    speclistdata = {
        "VIN": speclist['VIN'],
        "Line": speclist['Line'],
        "Date": speclist['Date'],
        "Time": speclist['Time'],
        "Spec": speclist['Spec']
    }

    print(speclist['VIN'])

    for item in new_labour:
        speclistdata["WS"] = item[0]
        speclistdata["WSTime"] = item[1]

        new_pc.send_msg(speclistdata)

