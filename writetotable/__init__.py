import logging
import requests
import json

import azure.functions as func


def main(message: func.ServiceBusMessage):

    device_id = "device01"
    # Log the Service Bus Message as plaintext
#{'PayLoadTimeStamp': '2020-05-27T12:31:01Z', 'ReverseTimeStamp': '8586110242237421568.000000', 'Payload': {'aq_temperature': 29.73, 'baro_altitude': 505.283, 'aq_iaq_accuracy': 1, 'motion_detector': 0, 'baro_temperature': 28.47, 'aq_air_pressure': 967.47, 'aq_iaq_index': 95, 'al_illuminance': 3.19, 'aq_humidity': 26.69, 'baro_airpressure': 969.637}​}
    message_content_type = message.content_type
    message_body = message.get_body().decode("utf-8")
    print(message_body)
    #convert string to dict
    message_body = str(message_body)
    message_body2 = message_body.replace("'", "\"")
    message_body_dict = json.loads(message_body2)
    payload = message_body_dict["Payload"]
    payload["TimeStamp"] = message_body_dict["PayLoadTimeStamp"]
    payload["ReverseTimeStamp"] = message_body_dict["ReverseTimeStamp"]

    motion_detector = {}
    motion_detector["PartitionKey"] = str(device_id + "_" + "motion_detector")
    motion_detector["RowKey"] = payload["ReverseTimeStamp"]
    motion_detector["Value"] = payload["motion_detector"]
    motion_detector["Device_Id"] = device_id
    motion_detector["Sensor_Id"] = "motion_detector"
    motion_detector["TimeStamp"] = payload["TimeStamp"]

    al_illuminance = {}
    al_illuminance["PartitionKey"] = str(device_id + "_" + "al_illuminance")
    al_illuminance["RowKey"] = payload["ReverseTimeStamp"]
    al_illuminance["Value"] = payload["al_illuminance"]
    al_illuminance["Device_Id"] = device_id
    al_illuminance["Sensor_Id"] = "al_illuminance"
    al_illuminance["TimeStamp"] = payload["TimeStamp"]

    aq_iaq_index = {}
    aq_iaq_index["PartitionKey"] = str(device_id + "_" + "aq_iaq_index")
    aq_iaq_index["RowKey"] = payload["ReverseTimeStamp"]
    aq_iaq_index["Value"] = payload["aq_iaq_index"]
    aq_iaq_index["Device_Id"] = device_id
    aq_iaq_index["Sensor_Id"] = "aq_iaq_index"
    aq_iaq_index["TimeStamp"] = payload["TimeStamp"]

    aq_humidity = {}
    aq_humidity["PartitionKey"] = str(device_id + "_" + "aq_humidity")
    aq_humidity["RowKey"] = payload["ReverseTimeStamp"]
    aq_humidity["Value"] = payload["aq_humidity"]
    aq_humidity["Device_Id"] = device_id
    aq_humidity["Sensor_Id"] = "aq_humidity"
    aq_humidity["TimeStamp"] = payload["TimeStamp"]

    baro_altitude = {}
    baro_altitude["PartitionKey"] = str(device_id + "_" + "baro_altitude")
    baro_altitude["RowKey"] = payload["ReverseTimeStamp"]
    baro_altitude["Value"] = payload["baro_altitude"]
    baro_altitude["Device_Id"] = device_id
    baro_altitude["Sensor_Id"] = "baro_altitude"
    baro_altitude["TimeStamp"] = payload["TimeStamp"]

    aq_air_pressure = {}
    aq_air_pressure["PartitionKey"] = str(device_id + "_" + "aq_air_pressure")
    aq_air_pressure["RowKey"] = payload["ReverseTimeStamp"]
    aq_air_pressure["Value"] = payload["aq_air_pressure"]
    aq_air_pressure["Device_Id"] = device_id
    aq_air_pressure["Sensor_Id"] = "aq_air_pressure"
    aq_air_pressure["TimeStamp"] = payload["TimeStamp"]

    baro_temperature = {}
    baro_temperature["PartitionKey"] = str(device_id + "_" + "baro_temperature")
    baro_temperature["RowKey"] = payload["ReverseTimeStamp"]
    baro_temperature["Value"] = payload["baro_temperature"]
    baro_temperature["Device_Id"] = device_id
    baro_temperature["Sensor_Id"] = "baro_temperature"
    baro_temperature["TimeStamp"] = payload["TimeStamp"]

    aq_temperature = {}
    aq_temperature["PartitionKey"] = str(device_id + "_" + "aq_temperature")
    aq_temperature["RowKey"] = payload["ReverseTimeStamp"]
    aq_temperature["Value"] = payload["aq_temperature"]
    aq_temperature["Device_Id"] = device_id
    aq_temperature["Sensor_Id"] = "aq_temperature"
    aq_temperature["TimeStamp"] = payload["TimeStamp"]

    baro_airpressure = {}
    baro_airpressure["PartitionKey"] = str(device_id + "_" + "baro_airpressure")
    baro_airpressure["RowKey"] = payload["ReverseTimeStamp"]
    baro_airpressure["Value"] = payload["baro_airpressure"]
    baro_airpressure["Device_Id"] = device_id
    baro_airpressure["Sensor_Id"] = "baro_airpressure"
    baro_airpressure["TimeStamp"] = payload["TimeStamp"]

    aq_iaq_accuracy = {}
    aq_iaq_accuracy["PartitionKey"] = str(device_id + "_" + "aq_iaq_accuracy")
    aq_iaq_accuracy["RowKey"] = payload["ReverseTimeStamp"]
    aq_iaq_accuracy["Value"] = payload["aq_iaq_accuracy"]
    aq_iaq_accuracy["Device_Id"] = device_id
    aq_iaq_accuracy["Sensor_Id"] = "aq_iaq_accuracy"
    aq_iaq_accuracy["TimeStamp"] = payload["TimeStamp"]

################################################################################################
    motion_detector_last = {}
    motion_detector_last["PartitionKey"] = str(device_id)
    motion_detector_last["RowKey"] = "motion_detector"
    motion_detector_last["Value"] = payload["motion_detector"]
    motion_detector_last["Device_Id"] = device_id
    motion_detector_last["Sensor_Id"] = "motion_detector"
    motion_detector_last["TimeStamp"] = payload["TimeStamp"]
    
    al_illuminance_last = {}
    al_illuminance_last["PartitionKey"] = str(device_id)
    al_illuminance_last["RowKey"] = "al_illuminance"
    al_illuminance_last["Value"] = payload["al_illuminance"]
    al_illuminance_last["Device_Id"] = device_id
    al_illuminance_last["Sensor_Id"] = "al_illuminance"
    al_illuminance_last["TimeStamp"] = payload["TimeStamp"]

    aq_iaq_index_last = {}
    aq_iaq_index_last["PartitionKey"] = str(device_id)
    aq_iaq_index_last["RowKey"] = "aq_iaq_index"
    aq_iaq_index_last["Value"] = payload["aq_iaq_index"]
    aq_iaq_index_last["Device_Id"] = device_id
    aq_iaq_index_last["Sensor_Id"] = "aq_iaq_index"
    aq_iaq_index_last["TimeStamp"] = payload["TimeStamp"]

    aq_humidity_last = {}
    aq_humidity_last["PartitionKey"] = str(device_id)
    aq_humidity_last["RowKey"] = "aq_humidity"
    aq_humidity_last["Value"] = payload["aq_humidity"]
    aq_humidity_last["Device_Id"] = device_id
    aq_humidity_last["Sensor_Id"] = "aq_humidity"
    aq_humidity_last["TimeStamp"] = payload["TimeStamp"]

    baro_altitude_last = {}
    baro_altitude_last["PartitionKey"] = str(device_id)
    baro_altitude_last["RowKey"] = "baro_altitude"
    baro_altitude_last["Value"] = payload["baro_altitude"]
    baro_altitude_last["Device_Id"] = device_id
    baro_altitude_last["Sensor_Id"] = "baro_altitude"
    baro_altitude_last["TimeStamp"] = payload["TimeStamp"]

    aq_air_pressure_last = {}
    aq_air_pressure_last["PartitionKey"] = str(device_id)
    aq_air_pressure_last["RowKey"] = "aq_air_pressure"
    aq_air_pressure_last["Value"] = payload["aq_air_pressure"]
    aq_air_pressure_last["Device_Id"] = device_id
    aq_air_pressure_last["Sensor_Id"] = "aq_air_pressure"
    aq_air_pressure_last["TimeStamp"] = payload["TimeStamp"]

    baro_temperature_last = {}
    baro_temperature_last["PartitionKey"] = str(device_id)
    baro_temperature_last["RowKey"] = "baro_temperature"
    baro_temperature_last["Value"] = payload["baro_temperature"]
    baro_temperature_last["Device_Id"] = device_id
    baro_temperature_last["Sensor_Id"] = "baro_temperature"
    baro_temperature_last["TimeStamp"] = payload["TimeStamp"]

    aq_temperature_last = {}
    aq_temperature_last["PartitionKey"] = str(device_id)
    aq_temperature_last["RowKey"] = "aq_temperature"
    aq_temperature_last["Value"] = payload["aq_temperature"]
    aq_temperature_last["Device_Id"] = device_id
    aq_temperature_last["Sensor_Id"] = "aq_temperature"
    aq_temperature_last["TimeStamp"] = payload["TimeStamp"]

    baro_airpressure_last = {}
    baro_airpressure_last["PartitionKey"] = str(device_id)
    baro_airpressure_last["RowKey"] = "baro_airpressure"
    baro_airpressure_last["Value"] = payload["baro_airpressure"]
    baro_airpressure_last["Device_Id"] = device_id
    baro_airpressure_last["Sensor_Id"] = "baro_airpressure"
    baro_airpressure_last["TimeStamp"] = payload["TimeStamp"]

    aq_iaq_accuracy_last = {}
    aq_iaq_accuracy_last["PartitionKey"] = str(device_id)
    aq_iaq_accuracy_last["RowKey"] = "aq_iaq_accuracy"
    aq_iaq_accuracy_last["Value"] = payload["aq_iaq_accuracy"]
    aq_iaq_accuracy_last["Device_Id"] = device_id
    aq_iaq_accuracy_last["Sensor_Id"] = "aq_iaq_accuracy"
    aq_iaq_accuracy_last["TimeStamp"] = payload["TimeStamp"]


    all_sensors = [
        motion_detector,
        al_illuminance,
        aq_iaq_index,
        aq_humidity,
        baro_altitude,
        aq_air_pressure,
        baro_temperature,
        aq_temperature,
        baro_airpressure,
        aq_iaq_accuracy]

    all_sensors_last = [
        motion_detector_last,
        al_illuminance_last,
        aq_iaq_index_last,
        aq_humidity_last,
        baro_altitude_last,
        aq_air_pressure_last,
        baro_temperature_last,
        aq_temperature_last,
        baro_airpressure_last,
        aq_iaq_accuracy_last]

    headers = {"content-type": "application/json"}

    for sensor in all_sensors:
        #convert dict to json
        sensor = json.dumps(sensor)
        x = requests.post("https://storageraumueberwachung.table.core.windows.net/Raw2?sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2023-07-31T22:47:36Z&st=2020-05-13T14:47:36Z&spr=https&sig=4skbsTU9tMlfSqFJhB1rTw16nVRIA6EVzxEkCmrNneE%3D", data=sensor, headers=headers)
        response = str(x.text)
        print("Response: " + response)

    for sensor_last in all_sensors_last:
        #convert dict to json
        partition_key = sensor_last["PartitionKey"]
        row_key = sensor_last["RowKey"]
        sensor_last = json.dumps(sensor_last)
        url = "https://storageraumueberwachung.table.core.windows.net/Last2(PartitionKey='{}', RowKey='{}')?sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2023-07-31T22:47:36Z&st=2020-05-13T14:47:36Z&spr=https&sig=4skbsTU9tMlfSqFJhB1rTw16nVRIA6EVzxEkCmrNneE%3D".format(partition_key, row_key)
        y = requests.put(url, data=sensor_last, headers=headers)
        response = str(y.text)
        print("Response: " + response)

    print("all done")
    #logging.info("Python ServiceBus topic trigger processed message.")
    #logging.info("Message Content Type: " + message_content_type)
    #logging.info("Message Body: " + message_body)