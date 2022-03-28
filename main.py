import argparse
import json
import sys
from typing import Any
import csv

import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as sub

headers_devices = ["client_id", "rssi", "txpower", "mac"]
headers_filtered = ["client_id", "rssi", "txpower", "mac", "x", "y"]
headers_position = ["client_id", "x", "y", "error", "date"]


def input_validation() -> dict:
    """A function that parses program parameters and checks their correctness"""
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("-p", "--port", type=int, required=True, help="(Enter the port number)")
    argument_parser.add_argument("-B", "--bind", type=str, required=True, help="(Enter the address of the broker)")
    argument_parser.add_argument("-t", "--topic", type=str, required=True, help="(Enter a topic)",
                                 choices=["devices", "filtered", "position"])

    args = vars(argument_parser.parse_args())

    return args

def on_message_devices(client: mqtt.Client, userdata: Any, message: Any) -> None:
    devices_json = json.loads(message.payload)
    array_devices = []

    temp_dict = {"client_id": devices_json["client_id"]}

    for device in devices_json["devices"]:
        temp_dict2 = {"rssi": device["rssi"],
                      "txpower": device["txpower"],
                      "mac": device["mac"]
                      }
        array_devices.append({**temp_dict, **temp_dict2})

    with open("./data/devices.csv", "a", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers_devices)
        writer.writerows(array_devices)

    array_devices.pop()


def on_message_filtered(client: mqtt.Client, userdata: Any, message: Any) -> None:
    filtered_json = json.loads(message.payload)
    array_filtered = []

    temp_dict = {"client_id": filtered_json["client_id"]}

    for rpi in filtered_json["rpi"]:
        temp_dict2 = {"rssi": rpi["rssi"],
                      "txpower": rpi["txpower"],
                      "mac": rpi["mac"],
                      "x": rpi["x"],
                      "y": rpi["y"]
                      }
        array_filtered.append({**temp_dict, **temp_dict2})

    with open("./data/filtered.csv", "a", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers_devices)
        writer.writerows(array_filtered)

    array_filtered.pop()


def on_message_position(client: mqtt.Client, userdata: Any, message: Any) -> None:
    position_json = json.loads(message.payload)
    array_position = []

    temp_dict = {"client_id": position_json["client_id"]}
    temp_dict3 = {"date": position_json["date"]}

    for coords in position_json["coordinates"]:
        temp_dict2 = {"x": coords["rssi"],
                      "y": coords["txpower"],
                      "error": coords["mac"]
                      }
        array_position.append({**temp_dict, **temp_dict2,**temp_dict3})

    with open("./data/position.csv", "a", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers_devices)
        writer.writerows(array_position)

    array_position.pop()


def set_up_client() -> mqtt.Client:
    """A function to create and connect a client to the MQTT broker"""
    client = mqtt.Client()

    try:
        client.connect(args["bind"], args["port"], 60)
    except ConnectionRefusedError:
        sys.exit("Rejected connection")

    return client


if __name__ == '__main__':
    args = input_validation()
    client = set_up_client()

    topic = args["topic"]
    if topic == "devices":

        with open("./data/devices.csv", "a", encoding="UTF8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers_devices)
            writer.writeheader()

        sub.callback(callback=on_message_devices, topics=["devices"], hostname=args["bind"],
                     port=args["port"])

    elif topic == "filtered":

        with open("./data/filtered.csv", "a", encoding="UTF8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers_devices)
            writer.writeheader()

        sub.callback(callback=on_message_filtered, topics=["filtered"], hostname=args["bind"],
                     port=args["port"])

    else:

        with open("./data/position.csv", "a", encoding="UTF8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers_devices)
            writer.writeheader()

        sub.callback(callback=on_message_position, topics=["position"], hostname=args["bind"],
                     port=args["port"])