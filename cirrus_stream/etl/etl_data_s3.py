import pandas as pd
import os
import glob
from cirrus_stream.etl.extract_transform_data import ETEngine
import shlex


def find_client_files(client_name: str, working_dir: str) -> list[str]:
    """
    Find client files based on the client name and working directory.

    :param client_name: The name of the client to search for.
    :param working_dir: The working directory where the files are located.
    :return: List of file paths that match the client name.
    """
    # Use wildcard glob to find files with '_log.json' suffix in the working directory
    file_paths = glob.glob(os.path.join(working_dir, '*_log.json'))

    # Filter the file paths to only include those that contain the client name
    matching_file_paths = [file_path for file_path in file_paths if client_name in file_path]

    return matching_file_paths


def send_file_aws(local_file_address, s3_address, client, object_name=None):
    """
    Sends a file to AWS S3 storage.

    :param local_file_address: The local file address.
    :type local_file_address: str
    :param s3_address: The S3 address.
    :type s3_address: str
    :param client: The client name.
    :type client: str
    :param object_name: The object name (optional).
    :type object_name: str
    :return: None
    """
    file_addresses = find_client_files(client, os.path.dirname(local_file_address))
    for file_address in file_addresses:
        if object_name is None:
            object_name = os.path.basename(file_address)
        # match up file names by splitting and assigning last file name
        partition_name = file_address.split('/')[-1]
        partition_names = partition_name.split('_')
        #        YYYY = partition_names[1]
        #        MM = partition_names[2]
        #        DD = partition_names[3]
        #        HH = partition_names[4]
        #        MM = partition_names[5]
        #        SS = partition_names[6]
        bucket_path = (s3_address + partition_names[0] + '/' + partition_names[1] + '/' + partition_names[2] +
                  '/' + partition_names[3] + '/' + partition_names[4] + '/' + partition_name)
        if 13 < int(partition_names[5]) < 17 or 27 < int(partition_names[5]) < 31 or 43 < int(
                partition_names[5]) < 47 or 56 < int(partition_names[5]) < 60:
            end_of_hour = True
        else:
            end_of_hour = False
        et_tool.add_or_append_local_client_files(client, file_address, partition_names[1], partition_names[2],
                                                 partition_names[3], partition_names[4], end_of_hour)
        try:
            os.system(shlex.quote(f"aws s3 mv {file_address} {bucket_path}"))
        except Exception:
            print('ClientError transferring S3 files')
            # log.log(error_message)


et_tool = ETEngine()
# Base addresses
local_address = '/home/ubuntu/data/'
s3_address = "s3://streamingawsbucket/data/"
# Base configuration
configuration_file = pd.read_csv('optconnect_config.csv')
# Set individual variables to make things more clear.
clients = configuration_file['Client']
IP = configuration_file['IP']
keys = configuration_file['PAuth']
for idx, client in enumerate(clients):
    if IP[idx] != '/n' and keys[idx] != '/n' and idx < 2:  # remove idx condition to move beyond labrat
        print(str(client))
        send_file_aws(local_address, s3_address, client)
