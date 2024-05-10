timeout 3s forticlient vpn connect OptClient-VPN -u YousolarDataStreaming -s
wait
timeout 10s python3 scp_data.py
timeout 2s forticlient vpn disconnect
exit 1
