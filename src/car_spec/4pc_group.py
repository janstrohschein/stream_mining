import datetime
from src.car_spec.KafkaPC import KafkaPC


class GroupPC(KafkaPC):
    def __init__(self, in_topic, in_group, in_schema_file, out_topic, out_schema_file):
        super().__init__(in_topic, in_group, in_schema_file, out_topic, out_schema_file)


new_pc = GroupPC('labourlist', 'group_by', 'labourlist.avsc', 'groupresult', 'groupresult.avsc')
group_by = {}
n_cars = 5
ws_max = 75

for msg in new_pc.consumer:

    labourlist = new_pc.decode_msg(msg)

    if labourlist["Line"] not in group_by:
        group_by[labourlist["Line"]] = {}

    if labourlist["WS"] not in group_by[labourlist["Line"]]:
        group_by[labourlist["Line"]][labourlist["WS"]] = {}
        group_by[labourlist["Line"]][labourlist["WS"]]["sum"] = 0
        group_by[labourlist["Line"]][labourlist["WS"]]["cars"] = []


    if len(group_by[labourlist["Line"]][labourlist["WS"]]["cars"]) == n_cars:
        group_by[labourlist["Line"]][labourlist["WS"]]["sum"]-= group_by[labourlist["Line"]][labourlist["WS"]]["cars"][0][2]
        group_by[labourlist["Line"]][labourlist["WS"]]["cars"].pop(0)

    data = (labourlist["VIN"], labourlist["Spec"], labourlist["WSTime"])
    group_by[labourlist["Line"]][labourlist["WS"]]["cars"].append(data)

    group_by[labourlist["Line"]][labourlist["WS"]]["sum"] += labourlist["WSTime"]

    if group_by[labourlist["Line"]][labourlist["WS"]]["sum"] > ws_max:
        current_sum = group_by[labourlist["Line"]][labourlist["WS"]]["sum"]
        print("VIN: ", str(labourlist["VIN"]), "Line: ", labourlist["Line"], "WS: ", labourlist["WS"], "Value: ",
              current_sum, labourlist["Time"], str(datetime.datetime.now().time()))
