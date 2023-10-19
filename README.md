# influx-writer

Tool to send data retrieved on a subprocess call to influxdb.

This tool was developped to be use with [inverter-poller](https://github.com/manio/skymax-demo) tool which extract data from inverter. It is then mapped into influxdb measurement and send to the influxdb instance configured with the config file. 
