timeout 3s forticlient vpn connect OptClient-VPN -u YousolarDataStreaming -s
wait
timeout 20s python3 transfer_data.py
timeout 2s forticlient vpn disconnect
exit 1
