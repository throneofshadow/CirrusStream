#!/usr/bin/bash bash
python3 instance_log_monitoring.py&

until bash forticlient_stream.sh; do
    timeout 10s python3 etl_data_s3.py
    sleep 30
done
