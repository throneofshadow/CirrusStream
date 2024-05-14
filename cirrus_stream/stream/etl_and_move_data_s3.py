"""
    @author: Brett Nelson, YouSolar Engineering

    @version: 1.0

    @description: This class is used to send raw data files to AWS S3 datalake. Naming convention is given as
    <client_name>_<YYYY-MM-DD_HH-MM-SS>_log.json. Data is saved in clientname/YYYY/MM/DD/filename_dtype_log.filetype
    for each client and day, as well as hourly data dumps for 'bronze' raw data sources.
    Unstructured data files are saved first using the send_file_aws function. Data is then structured appropriately
    into columnar files and sent to AWS S3 using the

    @notes: Currently only supported through CirrusStream.sh script.

"""
import pandas as pd
import glob
import subprocess
import shlex
from cirrus_stream.etl.extract_transform_data import ETEngine


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


def partition_filename(named_file, part_type='/') -> list:
    """
    Get the partition name from the file name.
    :param named_file: The file name to get the partition name from.
    :type named_file: str
    :param part_type: The partition type to get the partition name from.
    :type part_type: str
    :return: list of partition objects.
    :rtype: list
    """
    partition_name = named_file.split(part_type)[-1]
    partition_names = partition_name.split('_')
    return [partition_name, partition_names]


def etl_and_transfer_data(local_address_file, s3_bucket_address, data_client):
    """
    Sends multiple files to AWS S3 storage. Files are 'bronze' unstructured and unmodified json records,
    'silver' structured, and 'gold' structured and aggregated records. Specific ETL operations are handled by
    the ETEngine class. High-level ETL operations are handled by the SQLize class, which is called through
    ETEngine.

    :param local_address_file: The local address file.
    :type local_address_file: str
    :param s3_bucket_address: The S3 bucket address.
    :type s3_bucket_address: str
    :param data_client: The data client name.
    :type data_client: str
    :return: None
    """
    file_addresses = find_client_file(data_client, local_address_file)
    for file_address in file_addresses:
        # match up file names by splitting and assigning last file name
        [partition_name, partition_names] = partition_filename(file_address, part_type='/')
        #        Partition names keys:
        #        YYYY = partition_names[1]
        #        MM = partition_names[2]
        #        DD = partition_names[3]
        #        HH = partition_names[4]
        #        MM = partition_names[5]
        #        SS = partition_names[6]
        bucket = (s3_bucket_address + partition_names[1] + '/' + partition_names[2] +
                  '/' + partition_names[3] + '/Hour' + partition_names[4] + '/' + partition_name)
        csv_bucket_address = bucket.split('.json')[0] + 'silver_log.csv'
        if 13 < int(partition_names[5]) < 17 or 27 < int(partition_names[5]) < 31 or 43 < int(
                partition_names[5]) < 47 or 56 < int(partition_names[5]) < 60:
            end_of_hour = True
        else:
            end_of_hour = False
        # Call ETL Engine methods to perform basic ETL operations on raw json files
        et_tool.add_or_append_local_client_files(client, file_address, partition_names[1], partition_names[2],
                                                 partition_names[3], partition_names[4], end_of_hour)
        try:
            subprocess.run(shlex.quote("aws s3 mv " + file_address + " " + bucket))  # Move local json file to S3
            subprocess.run(shlex.qoute("aws s3 cp" + file_address + " " + csv_bucket_address))  # Copy local daily record csv file to S3
            # Here we can also add some logic to move other file types from ETL to S3, like gold records
            # subprocess.run(shlex.quote("aws s3 cp " + file_address + " " + parquet_bucket_address))
        except Exception:
            print('ClientError transferring S3 files')


et_tool = ETEngine()  # Initiate ETL Engine class methods
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
        etl_and_transfer_data(local_address, s3_file_path, client)
        # etl into csv format
        # append a local .csv file, send to S3 as silver record
        # perform ETL, save as parquet as gold record
