# -*- coding: utf-8 -*-

import argparse
import subprocess
import syslog
import json
from datetime import datetime, timedelta
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
            syslog.syslog(syslog.LOG_ERR, "Failed to load configuration: {}".format(e))
            raise e

        self.status_timer = datetime.now()
        self.influx_client = influxdb.InfluxDBClient(
            self.conf["influx"]["host"],
            self.conf["influx"]["port"],
            self.conf["influx"]["user"],
            self.conf["influx"]["password"],
            self.conf["influx"]["database"],
        )
        self.inverter_warning = self.PolWarningInverter()

        tmp = self.PolConfInverter()
        for key, value in self.conf["inverter_conf"].items():
            if key in tmp.items():
                tmp[key] = value
        self.inverter_current_conf = tmp

    def PolDataInverter(self):
        try:
            inverter_data = subprocess.check_output(
                [
                    self.conf["inverterPoller"]["venv"],
                    self.conf["inverterPoller"]["path"],
                    "-p",
                    "/dev/hidraw0",
                    "-o",
                    "json",
                    "-c",
                    "QPIGS",
                ]
            )
        except subprocess.CalledProcessError as e:
            syslog.syslog(
                syslog.LOG_ERR, "inverter_poller returned with error {}".format(e)
            )
            raise e
        response = json.loads(inverter_data.decode("utf-8"))
        if "validity_check" in response:
            raise ValueError(
                "Response unexpected: {}".format(response["validity_check"])
            )
        return response

    def PolConfInverter(self):
        try:
            inverter_data = subprocess.check_output(
                [
                    self.conf["inverterPoller"]["venv"],
                    self.conf["inverterPoller"]["path"],
                    "-p",
                    "/dev/hidraw0",
                    "-o",
                    "json",
                    "-c",
                    "QPIRI",
                ]
            )
        except subprocess.CalledProcessError as e:
            syslog.syslog(
                syslog.LOG_ERR, "inverter_poller returned with error {}".format(e)
            )
            raise e
        response = json.loads(inverter_data.decode("utf-8"))
        if "validity_check" in response:
            raise ValueError(
                "Response unexpected: {}".format(response["validity_check"])
            )
        return response

    def PolFlagInverter(self):
        try:
            inverter_data = subprocess.check_output(
                [
                    self.conf["inverterPoller"]["venv"],
                    self.conf["inverterPoller"]["path"],
                    "-p",
                    "/dev/hidraw0",
                    "-o",
                    "json",
                    "-c",
                    "QFLAG",
                ]
            )
        except subprocess.CalledProcessError as e:
            syslog.syslog(
                syslog.LOG_ERR, "inverter_poller returned with error {}".format(e)
            )
            raise e
        response = json.loads(inverter_data.decode("utf-8"))
        if "validity_check" in response:
            raise ValueError(
                "Response unexpected: {}".format(response["validity_check"])
            )
        return response

    def PolWarningInverter(self):
        try:
            inverter_data = subprocess.check_output(
                [
                    self.conf["inverterPoller"]["venv"],
                    self.conf["inverterPoller"]["path"],
                    "-p",
                    "/dev/hidraw0",
                    "-o",
                    "json",
                    "-c",
                    "QPIWS",
                ]
            )
        except subprocess.CalledProcessError as e:
            syslog.syslog(
                syslog.LOG_ERR, "inverter_poller returned with error {}".format(e)
            )
            raise e

        response = json.loads(inverter_data.decode("utf-8"))
        if "validity_check" in response.items():
            raise ValueError(
                "Response unexpected: {}".format(response["validity_check"])
            )
        return response

    def ApplyInverterConf(self):
        if "battery_type" in self.conf["inverter_conf"]:
            if self.conf["inverter_conf"]["battery_type"] == "AGM":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "PBT00",
                    ]
                )
            elif self.conf["inverter_conf"]["battery_type"] == "Flooded":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "PBT01",
                    ]
                )
            elif self.conf["inverter_conf"]["battery_type"] == "User":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "PBT02",
                    ]
                )
            else:
                syslog.syslog(
                    syslog.LOG_WARNING,
                    "battery_type not found {}".format(
                        self.conf["inverter_conf"]["battery_type"]
                    ),
                )

        if "charger_source_priority" in self.conf["inverter_conf"]:
            if self.conf["inverter_conf"]["charger_source_priority"] == "Utility first":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "PCP00",
                    ]
                )
            elif self.conf["inverter_conf"]["charger_source_priority"] == "Solar first":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "PCP01",
                    ]
                )
            elif (
                self.conf["inverter_conf"]["charger_source_priority"]
                == "Solar + utility"
            ):
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "PCP02",
                    ]
                )
            elif (
                self.conf["inverter_conf"]["charger_source_priority"]
                == "Only solar charging permitted"
            ):
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "PCP03",
                    ]
                )
            else:
                syslog.syslog(
                    syslog.LOG_WARNING,
                    "charger_source_priority type not found {}".format(
                        self.conf["inverter_conf"]["device_charger_priority"]
                    ),
                )

        if "output_source_priority" in self.conf["inverter_conf"]:
            if self.conf["inverter_conf"]["output_source_priority"] == "Utility first":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "POP00",
                    ]
                )
            elif self.conf["inverter_conf"]["output_source_priority"] == "Solar first":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "POP01",
                    ]
                )
            elif self.conf["inverter_conf"]["output_source_priority"] == "SBU first":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        "POP02",
                    ]
                )
            else:
                syslog.syslog(
                    syslog.LOG_WARNING,
                    "output_source_priority type not found {}".format(
                        self.conf["inverter_conf"]["battery_type"]
                    ),
                )

        pe_option = "PE"
        pd_option = "PD"
        if "buzzer" in self.conf["inverter_conf"]:
            if self.conf["inverter_conf"]["buzzer"]:
                pe_option = "{}a".format(pe_option)
            else:
                pd_option = "{}a".format(pd_option)
        if "overload_bypass" in self.conf["inverter_conf"]:
            if self.conf["inverter_conf"]["overload_bypass"]:
                pe_option = "{}b".format(pe_option)
            else:
                pd_option = "{}b".format(pd_option)
        if "power_saving" in self.conf["inverter_conf"]:
            if self.conf["inverter_conf"]["power_saving"]:
                pe_option = "{}j".format(pe_option)
            else:
                pd_option = "{}j".format(pd_option)
        if "overload_restart" in self.conf["inverter_conf"]:
            if self.conf["inverter_conf"]["overload_restart"]:
                pe_option = "{}u".format(pe_option)
            else:
                pd_option = "{}u".format(pd_option)
        if "over_temperature_restart" in self.conf["inverter_conf"]:
            if self.conf["inverter_conf"]["over_temperature_restart"]:
                pe_option = "{}v".format(pe_option)
            else:
                pd_option = "{}v".format(pd_option)
        inverter_data = ""
        try:
            if pe_option != "PE":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        pe_option,
                    ]
                )
            if pd_option != "PD":
                inverter_data = subprocess.check_output(
                    [
                        self.conf["inverterPoller"]["venv"],
                        self.conf["inverterPoller"]["path"],
                        "-p",
                        "/dev/hidraw0",
                        "-o",
                        "json",
                        "-c",
                        pd_option,
                    ]
                )

        except subprocess.CalledProcessError as e:
            syslog.syslog(
                syslog.LOG_ERR,
                "inverter_poller returned with error {} runnig PE: {} or PD: {} command: ".format(
                    e, pe_option, pd_option
                ),
            )
            raise e

        return json.loads(inverter_data.decode("utf-8"))

    def MapData(self, data):
        date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = [
            {
                "measurement": "battery",
                "tags": {"id": 1},
                "time": date,
                "fields": {
                    "DC_V": data["battery_voltage"],
                    "DC_V_scc": data["battery_voltage_from_scc"],
                    "charging_current": data["battery_charging_current"],
                    "discharge_current": data["battery_discharge_current"],
                    "soc": data["battery_capacity"],
                    "battery_voltage_to_steady_while_charging": data[
                        "is_battery_voltage_to_steady_while_charging"
                    ],
                },
            },
            {
                "measurement": "pv",
                "tags": {"id": 1},
                "time": date,
                "fields": {
                    "DC_V": data["pv_input_voltage"],
                    "A": data["pv_input_current_for_battery"],
                    "W": data["pv_input_power"],
                },
            },
            {
                "measurement": "grid",
                "tags": {"id": 1},
                "time": date,
                "fields": {
                    "AC_V": data["ac_input_voltage"],
                    "Hz": data["ac_input_frequency"],
                },
            },
            {
                "measurement": "out",
                "tags": {"id": 1},
                "time": date,
                "fields": {
                    "AC_V": data["ac_output_voltage"],
                    "Hz": data["ac_output_frequency"],
                    "load_watt": data["ac_output_active_power"],
                    "load_percent": data["ac_output_load"],
                    "load_va": data["ac_output_apparent_power"],
                },
            },
            {
                "measurement": "inverter",
                "tags": {"id": 1},
                "time": date,
                "fields": {
                    "bus_voltage": data["bus_voltage"],
                    "heat_sink_temperature": data["inverter_heat_sink_temperature"],
                    "load_status_on": data["is_load_on"],
                    "scc_charge_on": data["is_scc_charging_on"],
                    "ac_charge_on": data["is_ac_charging_on"],
                    "charging_on": data["is_charging_on"],
                    "charging_to_float": data["is_charging_to_float"],
                    "configuration_changed": data["is_configuration_changed"],
                    "switched_on": data["is_switched_on"],
                },
            },
        ]
        return payload

    def MapConfig(self, data):
        date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = [
            {
                "measurement": "config",
                "tags": {"id": 1},
                "time": date,
                "fields": {
                    "ac_input_voltage": data["ac_input_voltage"],
                    "ac_input_current": data["ac_input_current"],
                    "ac_output_voltage": data["ac_output_voltage"],
                    "ac_output_frequency": data["ac_output_frequency"],
                    "ac_output_apparent_power": data["ac_output_apparent_power"],
                    "ac_output_active_power": data["ac_output_active_power"],
                    "battery_voltage": data["battery_voltage"],
                    "battery_type": data["battery_type"],
                    "battery_recharge_voltage": data["battery_recharge_voltage"],
                    "battery_under_voltage": data["battery_under_voltage"],
                    "battery_bulk_charge_voltage": data["battery_bulk_charge_voltage"],
                    "battery_float_charge_voltage": data[
                        "battery_float_charge_voltage"
                    ],
                    "battery_redischarge_voltage": data["battery_redischarge_voltage"],
                    "input_voltage_range": data["input_voltage_range"],
                    "output_source_priority": data["output_source_priority"],
                    "charger_source_priority": data["charger_source_priority"],
                    "max_parallel_units": data["max_parallel_units"],
                    "max_ac_charging_current": data["max_ac_charging_current"],
                    "max_charging_current": data["max_charging_current"],
                    "machine_type": data["machine_type"],
                    "topology": data["topology"],
                    "output_mode": data["output_mode"],
                    "pv_ok_condition": data["pv_ok_condition"],
                    "pv_power_balance": data["pv_power_balance"],
                },
            }
        ]
        return payload

    def MapWarning(self, data):
        date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = [
            {
                "measurement": "warning",
                "tags": {"id": 1},
                "time": date,
                "fields": {
                    "bat_open_fault": data["bat_open_fault"],
                    "battery_low_alarm_warning": data["battery_low_alarm_warning"],
                    "battery_short_fault": data["battery_short_fault"],
                    "battery_too_low_to_charge_warning": data[
                        "battery_too_low_to_charge_warning"
                    ],
                    "battery_under_shutdown_warning": data[
                        "battery_under_shutdown_warning"
                    ],
                    "battery_voltage_to_high_fault": data[
                        "battery_voltage_to_high_fault"
                    ],
                    "bus_over_fault": data["bus_over_fault"],
                    "bus_soft_fail_fault": data["bus_soft_fail_fault"],
                    "bus_under_fault": data["bus_under_fault"],
                    "current_sensor_fail_fault": data["current_sensor_fail_fault"],
                    "eeprom_fault": data["eeprom_fault"],
                    "fan_locked_fault": data["fan_locked_fault"],
                    "inverter_fault": data["inverter_fault"],
                    "inverter_over_current_fault": data["inverter_over_current_fault"],
                    "inverter_soft_fail_fault": data["inverter_soft_fail_fault"],
                    "inverter_voltage_too_high_fault": data[
                        "inverter_voltage_too_high_fault"
                    ],
                    "inverter_voltage_too_low_fault": data[
                        "inverter_voltage_too_low_fault"
                    ],
                    "line_fail_warning": data["line_fail_warning"],
                    "mppt_overload_fault": data["mppt_overload_fault"],
                    "mppt_overload_warning": data["mppt_overload_warning"],
                    "op_dc_voltage_over_fault": data["op_dc_voltage_over_fault"],
                    "opv_short_warning": data["opv_short_warning"],
                    "over_temperature_fault": data["over_temperature_fault"],
                    "overload_fault": data["overload_fault"],
                    "power_limit_warning": data["power_limit_warning"],
                    "pv_voltage_high_warning": data["pv_voltage_high_warning"],
                    "reserved": data["reserved"],
                    "self_test_fail_fault": data["self_test_fail_fault"],
                },
            }
        ]
        return payload

    def InfluxWrite(self, payload):
        try:
            self.influx_client.write_points(payload)
            # print("Write points: {0}".format(payload))
        except influxdb.exceptions.InfluxDBClientError as e:
            syslog.syslog(
                syslog.LOG_WARNING,
                "{} database not found, intempting to create now".format(
                    self.conf["influx"]["database"]
                ),
            )
            self.influx_client.create_database(self.conf["influx"]["database"])
            # print("DB created, writing points: {0}".format(payload))
            self.influx_client.write_points(payload)

    def Run(self):
        deadline = datetime.now()
        now = datetime.now()
        syslog.syslog(syslog.LOG_INFO, "influx-writer started")

        failCount = 0
        try:
            self.inverter_warning = self.PolWarningInverter()
            tmp = self.PolConfInverter()
            for key, value in self.conf["inverter_conf"].items():
                if key in tmp.items():
                    tmp[key] = value
            self.inverter_current_conf = tmp
            payload = self.MapConfig(self.inverter_current_conf)
            self.InfluxWrite(payload)
            payload = self.MapWarning(self.inverter_warning)
            self.InfluxWrite(payload)
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, "Failed to poll inverter {}".format(e))
            failCount += 1

        while True:
            try:
                rawData = self.PolDataInverter()
                failCount = 0
            except Exception as e:
                syslog.syslog(
                    syslog.LOG_ERR, "Failed to poll data from inverter {}".format(e)
                )
                failCount += 1

            if deadline < now:
                try:
                    rawConf = self.PolConfInverter()
                    rawWarn = self.PolWarningInverter()
                except Exception as e:
                    syslog.syslog(
                        syslog.LOG_ERR,
                        "Failed to poll config from inverter {}".format(e),
                    )
                    failCount += 1

            if failCount == 0:
                payload = self.MapData(rawData)
                self.InfluxWrite(payload)
                if deadline < now:
                    deadline = self.status_timer + timedelta(minutes=15)
                    payload = self.MapConfig(rawConf)
                    self.InfluxWrite(payload)
                    syslog.syslog(
                        syslog.LOG_INFO, "send config payload {}".format(payload)
                    )

                    if sorted(self.inverter_current_conf.items()) != sorted(
                        rawConf.items()
                    ):
                        self.ApplyInverterConf()
                        payload = self.MapConfig(rawConf)
                        self.InfluxWrite(payload)

                    if (
                        sorted(self.inverter_warning.items()) != sorted(rawWarn.items())
                        or deadline < now
                    ):
                        self.inverter_warning = rawWarn
                        payload = self.MapWarning(rawWarn)
                        self.InfluxWrite(payload)

            elif failCount % 10 > 8:
                syslog.syslog(
                    syslog.LOG_ERR,
                    "Inverter polling failed in a {} time in a raw".format(failCount),
                )
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--conf", help="path to a config file")
    args = parser.parse_args()

    inverter = Inverter(args.conf)
    inverter.Run()
