# -*- coding: utf-8 -*-

import argparse
import subprocess
import syslog
import json
from datetime import datetime
import influxdb
import time

class Inverter:
    # init class loading config file value
    def __init__(self, config_path):
        try:
            with open(config_path, "r") as jsonfile:
                config = json.load(jsonfile)
                # store the wall config in this var to update the config file
                self.conf = config
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, 'Failed to load configuration: {}'.format(e))
            raise e
        # retreive api hello-asso api token to perform authenticate queries
        self.influx_client = influxdb.InfluxDBClient(
            self.conf["influx"]["host"], 
            self.conf["influx"]["port"], 
            self.conf["influx"]["user"], 
            self.conf["influx"]["password"], 
            self.conf["influx"]["database"]
        )


    def PolInverter(self):
        try:
            inverter_data = subprocess.check_output([self.conf["inverterPoller"]["path"], '-1'])
        except subprocess.CalledProcessError as e:
            syslog.syslog(syslog.LOG_ERR, 'inverter_poller returned with error {}'.format(e))
            raise e

        return json.loads(inverter_data.decode('utf-8'))

    def MapData(self, data):
        #TODO maybe store the mapping into the config file to change it easily without touching code
        date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = [
            {
                "measurement": "battery",
                "tags": {
                    "id": 1
                },
                "time": date,
                "fields": {
                    "DC_V": data['Battery_voltage'],
                    "DC_V_scc": data['SCC_voltage'],
                    "charging_current": data['Battery_charge_current'],
                    "discharge_current": data['Battery_discharge_current'],
                    "soc": data['Battery_capacity'],
                    "recharge_voltage": data['Battery_recharge_voltage'],
                    "under_voltage": data['Battery_under_voltage'],
                    "bulk_voltage": data['Battery_bulk_voltage'],
                    "float_voltage": data['Battery_float_voltage'],
                    "re_discharge_voltage": data['Battery_redischarge_voltage']
                }
            },
            {
                "measurement": "pv",
                "tags": {
                    "id": 1
                },
                "time": date,
                "fields": {
                    "DC_V": data['PV_in_voltage'],
                    "A": data['PV_in_current'],
                    "W": data['PV_in_watts'],
                    "Wh": data['PV_in_watthour']
                }
            },
            {
                "measurement": "grid",
                "tags": {
                    "id": 1
                },
                "time": date,
                "fields": {
                    "AC_V": data['AC_grid_voltage'],
                    "Hz": data['AC_grid_frequency'],
                    "max_grid_charge_current": data['Max_grid_charge_current']
                }
            },
            {
                "measurement": "out",
                "tags": {
                    "id": 1
                },
                "time": date,
                "fields": {
                    "AC_V": data['AC_out_voltage'],
                    "Hz": data['AC_out_frequency'],
                    "load_watt": data['Load_watt'],
                    "load_watthour": data['Load_watthour'],
                    "load_percent": data['Load_pct'],
                    "load_va": data['Load_va']
                }
            },
            {
                "measurement": "inverter",
                "tags": {
                    "id": 1
                },
                "time": date,
                "fields": {
                    "bus_voltage": data['Bus_voltage'],
                    "heat_sink_temperature": data['Heatsink_temperature'],
                    "max_charge_current": data['Max_charge_current'],
                    "inverter_mode": data['Inverter_mode'],
                    "load_status_on": data['Load_status_on'],
                    "scc_charge_on": data['SCC_charge_on'],
                    "ac_charge_on": data['AC_charge_on'],
                    "output_source_priority": data['Out_source_priority'],
                    "charger_source_priority": data['Charger_source_priority'],
                    "warnings": inverter.MapBitfieldToWarnings(data['Warnings'])
                }
            }
        ]
        return payload

    def InfluxWrite(self, payload):
        try:
            self.influx_client.write_points(payload)
            print("Write points: {0}".format(payload))
        except influxdb.exceptions.InfluxDBClientError as e:
            syslog.syslog(syslog.LOG_WARNING, '{} database not found, intempting to create now'.format(self.conf["influx"]["database"]))
            self.influx_client.create_database(self.conf["influx"]["database"])
            print("DB created, writing points: {0}".format(payload))
            self.influx_client.write_points(payload)

    def Run(self):
        syslog.syslog(syslog.LOG_INFO, "influx-writer started")
        while True:
            failCount = 0
            try:
                rawData = self.PolInverter()
                failCount = 0
            except Exception as e:
                syslog.syslog(syslog.LOG_ERR, 'Failed to poll inverter {}'.format(e))
                failCount += 1

            if failCount == 0:
                #TODO find a nice way to store activated warnings and make a use of MapBitfieldToWarnings function
                payload = self.MapData(rawData)
                self.InfluxWrite(payload)
            elif failCount > 3:
                syslog.syslog(syslog.LOG_ERR, '{} inverter polling failed in a raw, exiting process'.format(e))
                exit(-1)
            time.sleep(1)

    def MapBitfieldToWarnings(self, bitfield):
        # Define a dictionary mapping bit positions to warning messages
        bitfield_warnings = {
            0: "Reserved",
            1: "Fault : Inverter fault",
            2: "Fault : Bus Over",
            3: "Fault : Bus Under",
            4: "Fault : Bus Soft Fail",
            5: "Warning : LINE_FAIL",
            6: "Warning : OPVShort",
            7: "Fault : Inverter voltage too low",
            8: "Fault : Inverter voltage too high",
            9: "Fault (with bit 1)/warning : Over temperature",
            10: "Fault (with bit 1)/warning: Fan locked",
            11: "Fault (with bit 1)/warning: Battery voltage high",
            12: "Warning: Battery low alarm",
            13: "Reserved",
            14: "Battery under shutdown",
            15: "Reserved",
            16: "Fault (with bit 1)/warning: Over load",
            17: "Warning: Eeprom fault",
            18: "Fault: Inverter Over Current",
            19: "Fault: Inverter Soft Fail",
            20: "Fault: Self Test Fail",
            21: "Fault: OP DC Voltage Over",
            22: "Fault: Bat Open",
            23: "Fault: Current Sensor Fail",
            24: "Fault: Battery Short",
            25: "Warning: Power limit",
            26: "Warning: PV voltage high",
            27: "Warning: MPPT overload fault",
            28: "Warning: MPPT overload warning",
            29: "Warning: Battery too low to charge",
            30: "Reserved",
            31: "Reserved"
        }

        # Initialize an empty list to store activated warnings
        activated_warnings = ""

        # Iterate through each bit position in the 32-bit field
        for bit_position, bit in enumerate(bitfield):
            print(bit)
            # Check if the bit at the current position is activated (1)
            if bit == "1":
                if activated_warnings == "":
                    activated_warnings = bitfield_warnings[bit_position]
                else:
                    activated_warnings += ", {}".format(bitfield_warnings[bit_position])

        return activated_warnings

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--conf', help='path to a config file')
    args = parser.parse_args()

    inverter = Inverter(args.conf)
    inverter.Run()
