#!/usr/bin/bash bash
python3 monitoring/instance_log_monitoring.py&
until bash stream/forticlient_stream.sh; do
    timeout 20s python3 stream/transform_and_move_data_s3.py
    sleep 10
done
