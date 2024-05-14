"""
    @author: Brett Nelson, Yousolar Engineering

    @version: 1.0

    @description: This class is used to transfer raw data files from a PowerBlock client configured to either
    a) an external VPN network or b) an internal VPN network able to connect with clients using the ssh protocol.
    Preferred file transfer protocol is SCP currently, due to the ability to connect with clients using the ssh protocol.

    @notes: Currently only supported through CirrusStream.sh script. Must use in conjunction with a FortiClient VPN network.
    For more information please see the forticlient documentation as well as a set up script found in forticlient_installation.sh.


"""
import paramiko
from paramiko import SSHClient
from scp import SCPClient
import pandas as pd
from datetime import datetime, timezone


def get_ssh_scp_files(ssh_host, ssh_user, ssh_password, source_volume, destination_volume):
    """ An SCP file transfer function which takes as arguments a host, a user, >
        as well as source and destination file target paths.
        Credit in solution to https://stackoverflow.com/questions/43577248/scp-in->

        :param ssh_host: The SSH host.
        :param ssh_user: The SSH user.
        :param ssh_password: The SSH password.
        :param source_volume: The source volume.
        :param destination_volume: The destination volume.
        :return: None


    """
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ssh_host, username=ssh_user, password=ssh_password, look_for_keys=False)
    with SCPClient(ssh.get_transport()) as scp:
        try:
            scp.get(source_volume, destination_volume)
            ssh.close()
            return "good"
        except Exception as e:
            try:
                scp.get(source_volume, destination_volume)
                ssh.close()
                return "good"
            except Exception as e:
                print(ssh_host + ssh_user + ssh_password)
                ssh.close()
                return "bad"


# Import Client information
configuration_file = pd.read_csv('optconnect_config.csv')
# Set individual variables to make things more clear.
clients = configuration_file['Client']
IP = configuration_file['IP']
keys = configuration_file['PAuth']
# Set transfer file path
transfer_file_path = '/mnt/rwdata/Traffic/Data/traffic.json'
# Set local file path for transfer including date-time of scp
local_file_path = '/home/ubuntu/data/'
rt = str(datetime.now(timezone.utc).astimezone())[0:19]  # Time for iterative f>
# Datetime Format YYYY-MM-DD HH:MM:SS, using UTC time
for idx, client in enumerate(clients):
    if IP[idx] != '/n' and keys[idx] != '/n' and idx < 3:
        local_file_path_int = (local_file_path + str(client) + '_' + rt[0:4] + '_' + rt[5:7]
                               + '_' + rt[8:10] + '_' + rt[11:13] +
                               '_' + rt[14:16] + '_log.json')
        try:
            rv = get_ssh_scp_files(str(IP[idx]), 'root', str(keys[idx]),
                                   transfer_file_path, local_file_path_int)
            print(rv + str(idx))
        except Exception:
            rv = "bad"
            print(rv + str(idx))
        with open(local_file_path + 'streamlog.txt', 'a') as f:
            f.write(client + ',' + str(rv) + ',' + rt[0:17] + '\n')  # touch file and ensure monitoring
f.close()
