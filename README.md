# NaviMonkey debugger

A tool to debug and check for poor performance areas in NaviMonkey.

## Installation

The program requires a working MQTT broker. <br/>
Use the package manager [pip3](https://pip.pypa.io/en/stable/) to install NaviMonkey debugger.

```bash
pip3 install -r requirements.txt
```

## Usage

```
python3 main.py -B ADDRESS -p PORT -t TOPIC

Available topic: devices, filtered, position
```