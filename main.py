import argparse
import json
import signal
import sys
from typing import Any
import csv
import statistics

import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as sub


def n_generator():
    i = 1.5
    while i <= 7:
        yield i
        i += 0.10


# Globals
topic = ""

# devices
headers_devices = ["client_id", "rssi", "txpower", "mac"]
final_array_devices = []

# filtered
headers_filtered = ["mac", "min_rssi", "max_rssi", "mean_rssi", "median_rssi", "txpower", " ", "N"]
final_array_filtered = []
filtered_unique_mac = []
filtered_temp_array = []
N = [float(f"{i:.1f}") for i in n_generator()]

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
            "mac": rpi["mac"],
            "rssi": rpi["rssi"],
            "txpower": rpi["txpower"]
        })


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
            temp_txpower = 1
            for element in filtered_temp_array:
                if address == element["mac"]:
                    if temp_txpower == 1:
                        temp_txpower = element["txpower"]
                    temp_rssi_array.append(element["rssi"])
            temp_cos = {"mac": address, "rssi": temp_rssi_array, "txpower": temp_txpower}
            temp_rssi_array = []
            temp_cos_array.append(temp_cos)

        for mac in filtered_unique_mac:
            headers_filtered.append(mac)

        distance_dict = {}
        for index, header in enumerate(headers_filtered):
            if index <= 7:
                continue
            for element in temp_cos_array:
                if element["mac"] == header:

                    distance_dict[header] = (10 ** ((element["txpower"] - statistics.mean(element["rssi"])) / (10 * n))) * 100



        for element in temp_cos_array:
            final_array_filtered.append(
                {
                    "mac": element["mac"],
                    "min_rssi": max(element["rssi"]),
                    "max_rssi": min(element["rssi"]),
                    "mean_rssi": statistics.mean(element["rssi"]),
                    "median_rssi": statistics.median(element["rssi"]),
                    "txpower": element["txpower"],
                    " ": " ",
                    "N": N,

                }
            )

        with open("./data/filtered.csv", "w", encoding="UTF8", newline="") as f:
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
