import pandas as pd
import os
import glob
from botocore.exceptions import ClientError
from cirrus_stream.database.database_engine import DatabaseEngine



def find_client_file(client_name, working_dir):
    rf = []
    for files in glob.glob(working_dir+'*_log.json'): # use wildcard glob to fi>
        if client_name in files:
            rf.append(files)
    return rf
def send_file_aws(local_address, s3_address, client, object_name=None):
    #if os.environ.get('LC_CTYPE', '') == 'UTF-8':
    #    os.environ['LC_CTYPE'] = 'en_US.UTF-8'
    #driver = create_clidriver()  # Creates staging driver for terminal CLI
    #s3_client = boto3.client('s3')
    file_addresses = find_client_file(client, local_address)
    for file_address in file_addresses:
        if object_name is None:
           object_name = os.path.basename(file_address)
        # match up file names by splitting and assigning last file name
        partition_name = file_address.split('/')[-1]
        partition_names = partition_name.split('_')
        print(partition_names)
#        YYYY = partition_names[1]
#        MM = partition_names[2]
#        DD = partition_names[3]
#        HH = partition_names[4]
        #MM = partition_names[5]
        #SS = partition_names[6]
        bucket = (s3_address + partition_names[0] + '/' +  partition_names[1] + '/' + partition_names[2] +
                  '/' + partition_names[3] + '/' + partition_names[4] +'/' +  partition_name)
        if 13 < int(partition_names[5]) < 17 or 27 < int(partition_names[5]) < 31 or 43 < int(partition_names[5]) < 47 or 56 < int(partition_names[5]) < 60:
            end_of_hour = True
        else:
            end_of_hour=False
        db_tool.add_or_append_local_client_files(client, file_address, partition_names[1], partition_names[2], partition_names[3], partition_names[4], end_of_hour)
        try:
            os.system("aws s3 mv " + file_address + " " + bucket )
            #response = s3_client.upload_file(file_address, s3_address, Body=fi>
        except ClientError as e:
            print('ClientError transferring S3 files')

db_tool = DatabaseEngine()
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
    if IP[idx] != '/n' and keys[idx] != '/n' and idx < 2:  # remove idx conditi>
        #s3_file_path = s3_address + str(client) + '/'  # Current day folder
        print( str(client))
        send_file_aws(local_address, s3_address, client)
