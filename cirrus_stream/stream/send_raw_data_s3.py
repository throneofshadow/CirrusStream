"""
    @author: Brett Nelson, Yousolar Engineering

    @version: 1.0

    @description: This class is used to send raw data files to AWS S3 datalake. Naming convention is given as
    <client_name>_<YYYY-MM-DD_HH-MM-SS>_log.json. Data is saved in clientname/YYYY/MM/DD/file1.json.
    Data saved using this class is in JSON format and intended to yield bronze level data.

    @notes: Currently only supported through CirrusStream.sh script.

"""
import pandas as pd
import os
import glob
import subprocess
import shlex


def find_client_file(client_name, working_dir):
    """
    Find client files based on the client name and working directory.

    :param client_name: The name of the client to search for.
    :param working_dir: The working directory where the files are located.
    :return: List of file paths that match the client name.
    """
    rf = []
    for files in glob.glob(working_dir + '*_log.json'):  # use wildcard glob to filter by client name
        if client_name in files:
            rf.append(files)
    return rf


def send_file_aws(local_address_file, s3_bucket_address, data_client, object_name=None):
    """
    Sends a file to AWS S3 storage.

    :param local_address_file: The local address file.
    :type local_address_file: str
    :param s3_bucket_address: The S3 bucket address.
    :type s3_bucket_address: str
    :param data_client: The data client name.
    :type data_client: str
    :param object_name: The object name (optional).
    :type object_name: str
    :return: None
    """
    file_addresses = find_client_file(data_client, local_address_file)
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
        bucket = (s3_bucket_address + partition_names[1] + '/' + partition_names[2] +
                  '/' + partition_names[3] + '/Hour' + partition_names[4] + '/' + partition_name)
        try:
            result = subprocess.run(shlex.quote("aws s3 mv " + file_address + " " + bucket))
        except Exception:
            print('ClientError transferring S3 files')


# Base addresses
local_address = '/home/ubuntu/'
s3_address = "s3://streamingawsbucket/data/"
# Base configuration
configuration_file = pd.read_csv('optconnect_config.csv')

# Set individual variables to make things more clear.
clients = configuration_file['Client']
IP = configuration_file['IP']
keys = configuration_file['PAuth']

for idx, client in enumerate(clients):
    if IP[idx] != '/n' and keys[idx] != '/n' and idx < 1:  # remove idx condition to move beyond labrat
        s3_file_path = s3_address + str(client) + '/'  # Current day folder
        send_file_aws(local_address, s3_file_path, client)
        # etl into csv format
        # append a local .csv file, send to S3 as silver record
        # perform ETL, save as parquet as gold record
