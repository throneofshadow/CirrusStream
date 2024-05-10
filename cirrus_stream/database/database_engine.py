import pdb
import pandas as pd
import json
import glob
import os


def clean_json(file_address):
    """ Function intended to clean an un-parsed data record streamed from Powercon. Not to be
        used with 'clean' data files.
    """
    with open(file_address, 'a+') as f:
        f.seek(f.tell() - 1, os.SEEK_SET)
        f.truncate()
        f.write('\n')
        f.write(']')
    f.close()
    with open(file_address, 'r+') as f:
        f.seek(0, 0)  # Seek to first position
        f.write('[\n{')
    f.close()
    return


def open_json_file(file_address):
    """ Function intended to provide cleaning, reading functionality for json data structures.
        Json data structures are un-even w.r.t. each structure, however are consistent in structure between
        record type. Json data structures must be cleaned (remove trailing comma, add list indices) before
        reading can occur. Use the 'clean_json' function.
        Returns a list of json structures used for PowerBlock data records.

    """
    with open(file_address) as f:
        try:
            return json.load(f)
        except:
            f.close()  # close file attempting to be cleaned.
            try:
                print('Sanitizing JSON file. ')
                clean_json(file_address)
                with open(file_address) as ff:
                    return json.load(ff)
            except:
                print('cannot load file ' + str(file_address))
                return IOError


class DatabaseEngine:
    """ Set of methods distributed inside a class instance allowing a user to parse, load, modify, and
    save a set of .json files streamed from a PowerBlock client. These methods allow for """

    def __init__(self, current_file_exists=False, path_prefix='/home/ubuntu/data/'):
        self.file_address = None
        self.client_csv_file_address = {}
        self.client_csv_data = {}
        self.current_file_exists = current_file_exists
        self.path_prefix = path_prefix
        self.client_json_data = None
        self.updated_df = {}
        self.new_data_flag = False
        self.new_file = False
        self.bad_file = False
        self.s3_prefix = 's3://streamingbucketaws/data/'

    def add_or_append_local_client_files(self, client, file_address, YY, MM, DD, HH, end_of_hour=False):
        self.find_current_csv_data(client, file_address, YY, MM, DD, HH)
        self.append_and_merge_data_structures(client=client)
        self.save_local_client_file(client=client)
        if end_of_hour:
            self.move_hourly_file_to_s3(date_string = [YY, MM, DD, HH])
        # Check file date, move to S3 if necessary, log

    def find_current_csv_data(self, client, file_address, YY, MM, DD, HH):
        """Method to find current csv records on a per-client basis in a local file system (EC2 or otherwise). Method falls
       back on creating and loading into memory new data files if a log file is not found due to scheduled uploading, pruning etc.
       Method is intended to be called from python script giving local name files.
        """
        try:
            self.client_json_data = open_json_file(file_address)  # Returns a s>
        except:  # empty.
            self.client_json_data = []
            print('Cant load json data')
        # rv: file_address, client, DD
        self.daystring = '_' + DD
        #pdb.set_trace()
        if len(glob.glob(self.path_prefix + '*' + client +'*' + self.daystring + '*_log.csv')) > 0:
            print('Found csv file')
            self.current_file_exists = True
            self.new_file = False
            for files in glob.glob(self.path_prefix + '*' + client + '*'+ self.daystring + '*_log.csv'):
                self.client_csv_file_address[client] = files
                self.client_csv_data[client] = pd.read_csv(files)
                print(self.client_csv_data[client].shape)
                print('loaded dataframe shape')
        else:
            print('No current csv log file')
            self.current_file_exists = False
            self.new_file = True
            self.client_csv_file_address[client] = (self.path_prefix + client + '_' +
                                                YY + '_' + MM + '_' + DD + '_log.csv')
            self.client_csv_data[client] = pd.DataFrame()
            self.current_file_exists = True
        if self.new_file:
            print(self.current_file_exists, self.new_file)

    def append_and_merge_data_structures(self, client):
        try:
            new_data = pd.DataFrame(self.client_json_data)
            print(new_data.shape)
            print('shape of incoming data')
            self.new_data_flag = True
        except:
            if self.new_file is True:
                print('no existing file found, bad data for client. breaking loop')
                self.bad_file = True
                self.new_data_flag = None
                new_data = [1]
            else:
                new_data = [1]
                self.new_data_flag = None
                print('bad file, dont add to existing file.')
        # Load current data, or generate new dataframe object
        if self.current_file_exists is True and self.new_data_flag:
            data_file = self.client_csv_data[client]
            self.updated_df[client] = pd.concat([data_file, new_data]).drop_duplicates()
            print(data_file.shape)
            print('current data file shape')
            print(self.updated_df[client].shape)
            print('updated data file shape')
        else:
            print('no file')  # Generate new dataframe object for concat event
        #        pdb.set_trace()

    def save_local_client_file(self, client):
        if not self.new_data_flag:
            print('bad file')
        else:
            self.updated_df[client].to_csv(self.client_csv_file_address[client], index=False)

    def save_files_on_exit(self):
        for client, file_locations in self.client_csv_file_address.items():
            self.updated_df[client].to_csv(file_locations, index=False)  # Save over previous DF with concat version

    def move_hourly_file_to_s3(self, date_string):
        for client, file_location in self.client_csv_file_address.items():
            file_name = file_location.split('/')[-1]  # Get last file address.
            s3_address = ('s3://streamingawsbucket/data/' + client + '/' + date_string[0] + '/' +
                          date_string[1] + '/' + date_string[2] + '/' + file_name)
            os.system('aws s3 cp ' + file_location + ' ' + s3_address)  # use CLI to move file into S3


DatabaseEngine()
