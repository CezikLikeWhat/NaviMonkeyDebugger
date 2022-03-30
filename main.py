import argparse
import json
import signal
import sys
from typing import Any
import csv
import statistics 

import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as sub

# headers_filtered = ["client_id", "rssi", "txpower", "mac", "x", "y"," ", "Min-RSSI", "Max-RSSI", "Mean-RSSI", "Median-RSSI","TxPower"]

# Globals
topic = ""

# devices
headers_devices = ["client_id", "rssi", "txpower", "mac"]
final_array_devices = []

# filtered
headers_filtered = ["Min-RSSI", "Max-RSSI", "Mean-RSSI", "Median-RSSI", "TxPower"]
final_array_filtered = []
filtered_unique_mac = []
filtered_temp_array = []

# position
headers_position = ["client_id", "x", "y", "error", "date"]
final_array_position = []


def input_validation() -> dict:
    global topic
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument(
        "-p", "--port", type=int, required=True, help="(Enter the port number)"
    )
    argument_parser.add_argument(
        "-B",
        "--bind",
        type=str,
        required=True,
        help="(Enter the address of the broker)",
    )
    argument_parser.add_argument(
        "-t",
        "--topic",
        type=str,
        required=True,
        help="(Enter a topic)",
        choices=["devices", "filtered", "position"],
    )

    args = vars(argument_parser.parse_args())
    topic = args["topic"]

    return args


def on_message_devices(client: mqtt.Client, userdata: Any, message: Any) -> None:
    devices_json = json.loads(message.payload)
    array_devices = []

    temp_dict = {"client_id": devices_json["client_id"]}

    for device in devices_json["devices"]:
        temp_dict2 = {
            "rssi": device["rssi"],
            "txpower": device["txpower"],
            "mac": device["mac"],
        }
        array_devices.append({**temp_dict, **temp_dict2})

    with open("./data/devices.csv", "a", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers_devices)
        writer.writerows(array_devices)

    array_devices.pop()


def on_message_filtered(client: mqtt.Client, userdata: Any, message: Any) -> None:
    global final_array_filtered

    filtered_json = json.loads(message.payload)

    for rpi in filtered_json["rpi"]:
        if rpi["mac"] not in filtered_unique_mac:
            filtered_unique_mac.append(rpi["mac"])

        filtered_temp_array.append({
            "mac":rpi["mac"], 
            "rssi": rpi["rssi"], 
            "txpower": rpi["txpower"]
        })
    

    # temp_dict = {"client_id": filtered_json["client_id"]}

    # for rpi in filtered_json["rpi"]:

    #     if rpi["mac"] not in filtered_unique_mac:
    #         filtered_unique_mac.append(rpi["mac"])

    #     filtered_rssi.append(rpi["rssi"])

    #     temp_dict2 = {"rssi": rpi["rssi"],
    #                   "txpower": rpi["txpower"],
    #                   "mac": rpi["mac"],
    #                   "x": rpi["x"],
    #                   "y": rpi["y"]
    #                   }
    #     array_filtered.append({**temp_dict, **temp_dict2})

    # with open("./data/filtered.csv", "a", encoding="UTF8", newline="") as f:
    #     writer = csv.DictWriter(f, fieldnames=headers_devices)
    #     writer.writerows(array_filtered)

    # array_filtered.pop()


def on_message_position(client: mqtt.Client, userdata: Any, message: Any) -> None:
    position_json = json.loads(message.payload)
    array_position = []

    temp_dict = {"client_id": position_json["client_id"]}
    temp_dict3 = {"date": position_json["date"]}

    for coords in position_json["coordinates"]:
        temp_dict2 = {
            "x": coords["rssi"],
            "y": coords["txpower"],
            "error": coords["mac"],
        }
        array_position.append({**temp_dict, **temp_dict2, **temp_dict3})

    with open("./data/position.csv", "a", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers_devices)
        writer.writerows(array_position)

    array_position.pop()


def set_up_client() -> mqtt.Client:
    client = mqtt.Client()

    try:
        client.connect(args["bind"], args["port"], 60)
    except ConnectionRefusedError:
        sys.exit("Rejected connection")

    return client


def exit_handler(signum, frame):
    global topic, filtered_unique_mac, filtered_dict_of_everything
    if topic == "devices":

        with open("./data/devices.csv", "w", encoding="UTF8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers_devices)
            writer.writeheader()
            writer.writerows(final_array_devices)
    elif topic == "filtered":
        
        temp_cos_array = []
        temp_rssi_array = []
        for address in filtered_unique_mac:
            for element in filtered_temp_array:
                if address == element["mac"]:
                    temp_rssi_array.append(element["rssi"])
            temp_cos = {"mac": address,"rssi": temp_rssi_array,"txpower": element["txpower"]}
            temp_cos_array.append(temp_cos)
        
        
        for element in temp_cos_array:
            final_array_filtered.append(
                {
                    "mac":element["mac"],
                    "min_rssi": min(element["rssi"]),
                    "max_rssi": max(element["rssi"]),
                    "mean_rssi": statistics.mean(element["rssi"]),
                    "median_rssi": statistics.median(element["rssi"]),
                    "txpower": element["txpower"]
                }
            )
        print(final_array_filtered)
        with open("./data/filtered.csv", "a", encoding="UTF8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers_filtered)
            writer.writeheader()
            writer.writerows(final_array_filtered)


    else:
        
        with open("./data/position.csv", "w", encoding="UTF8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers_position)
            writer.writeheader()
            writer.writerows(final_array_position)

    sys.exit("Poprawnie zamknięto program!")


signal.signal(signal.SIGINT, exit_handler)

if __name__ == "__main__":
    args = input_validation()
    client = set_up_client()

    if topic == "devices":
        sub.callback(callback=on_message_devices, topics=["devices"], hostname=args["bind"],
                     port=args["port"])
    elif topic == "filtered":
        sub.callback(callback=on_message_filtered, topics=["filtered"], hostname=args["bind"],
                     port=args["port"])
    else:
        sub.callback(callback=on_message_position, topics=["position"], hostname=args["bind"],
                     port=args["port"])
