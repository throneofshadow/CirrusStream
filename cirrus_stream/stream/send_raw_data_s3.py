import pandas as pd
import os
import glob
import subprocess


def find_client_file(client_name, working_dir):
    rf = []
    for files in glob.glob(working_dir + '*_log.json'):  # use wildcard glob to filter by client name
        if client_name in files:
            rf.append(files)
    return rf


def send_file_aws(local_address_file, s3_bucket_address, data_client, object_name=None):
    """

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
            result = subprocess.run("aws s3 mv " + file_address + " " + bucket, shell=True)
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
