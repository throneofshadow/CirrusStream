#!/usr/bin/bash bash
python3 instance_log_monitoring.py&
int sleep_time = 10
until bash forticlient_stream.sh; do
    timeout 20s python3 etl_data_s3.py
    sleep sleep_time
done
