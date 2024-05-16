"""
    @author: Brett Nelson, Yousolar Engineering

    @version: 1.0

    @description: This class is used to parse and construct data structures streamed on AWS from
    the Powercon firehose to local EC2 or EKS instances. Currently, both reading from local files and accepting
    streamed data are both acceptable methods of interacting with ETLEngine.

    @notes: Currently supported databases are simple pandas data structures, parquet, and duckdb.


"""
import pdb
import pandas as pd
import json
import glob
import os
import shlex
from transform_csv_data_into_db import DatabaseFormatter as dbf
pd.options.mode.string_storage = "pyarrow"  # use pyarrow as string storage backend to improve speed


def test_json(file_address):
    """ Function intended to test if a json file is valid and able to be parsed using the 'read_json' function.
        If the file is not loaded, an exception is raised for the caller to handle.
        @type file_address: str
        @param file_address : The address of the file to be tested.
        @return: Boolean        @returns: Boolean
    """
    try:
        open_json_file(file_address, clean=False)
        return True
    except Exception:
        return False


def clean_json(file_address):
    """ Function intended to clean an un-parsed data record streamed from Powercon. Not to be
        used with 'clean' data files. This function loads a json file and removes the trailing newline.
        @type file_address: str
        @param file_address : The address of the file to be cleaned.
        @return: None        @returns: None

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


def open_json_file(file_address, clean=False):
    """ Function intended to provide cleaning, reading functionality for json data structures.
        Json data structures are un-even w.r.t. each structure, however are consistent in structure between
        record type. Json data structures must be cleaned (remove trailing comma, add list indices) before
        reading can occur. Use the 'clean_json' function.
        Returns a list of json structures used for PowerBlock data records.
        @param clean: Boolean, clean data or not
        @type clean: Boolean
        @type file_address: str
        @param file_address : The address of the file to be cleaned.
        @return: list        @returns: list of json structures

    """
    with open(file_address) as f:
        try:
            return json.load(f)
        except Exception:
            f.close()  # close file attempting to be cleaned.
            if clean:
                try:
                    print('Sanitizing JSON file. ')
                    clean_json(file_address)
                    with open(file_address) as ff:
                        data = json.load(ff)
                        ff.close()
                        return data
                except Exception:
                    print('cannot load file ' + str(file_address))
                    return IOError


class ETEngine:
    """ Set of methods distributed inside a class instance allowing a user to parse, load, modify, and
    save a set of .json files streamed from a PowerBlock client. Parsing and modification are called primarily
    using the cirrus_stream.sh script. The class instance is intended to be used in conjunction with a local file
    system to store and load data. For non-local data, see 'extract_transform_data_datalake.py'. """

    def __init__(self, client, file_address, YY, MM, DD, current_file_exists=False, path_prefix='/home/ubuntu/data/'):
        """
        Initializes the  class instance with default values for various attributes.

        Parameters:
        current_file_exists(bool): A boolean value indicating if a current file already exists.
        path_prefix(str): The path prefix for the local file system.
        Returns:
        None
        """
        self.client = client
        self.file_address = file_address
        self.year = YY
        self.month = MM
        self.day = DD
        self.current_file_exists = current_file_exists
        self.path_prefix = path_prefix
        # initialize attributes of class method
        self.file_address = None
        self.client_csv_file_address = None
        self.client_csv_data = None
        self.client_json_data = None
        self.day_string = None
        self.updated_df = None
        self.new_data_flag = False
        self.DB_connection = None
        self.new_file = False
        self.bad_file = False
        self.s3_prefix = 's3://streamingbucketaws/data/'
        # Initialize data read
        self.load_json_file()
        # Load or create current csv data
        self.find_or_create_current_csv_data()

    def add_or_append_local_client_csv_files(self):
        self.append_and_merge_initial_csv_data()  # Append and merge csv files (silver)
        self.save_local_client_file()  # Save local csv file for transfer to S3, reading into DBF
        # initialize DBFormatter class, un-necessary if using DBF for formatting and interaction with data
        self.DB_connection = dbf(self.client_csv_file_address[self.client])

    def load_json_file(self):
        try:
            self.client_json_data = open_json_file(self.file_address)  # Returns a s>
        except Exception:  # empty.
            self.client_json_data = []
            print('Cant load json data')

    def find_or_create_current_csv_data(self, verbose=True):
        self.day_string = '_' + self.day
        if len(glob.glob(self.path_prefix + '*' + self.client + '*' + self.day_string + '*_silver_log.csv')) > 0:
            if verbose:
                print('Found csv file')
            self.current_file_exists = True
            self.new_file = False
            for files in glob.glob(self.path_prefix + '*' + self.client + '*' + self.day_string + '*_silver_log.csv'):
                self.client_csv_file_address[self.client] = files
                self.client_csv_data[self.client] = pd.read_csv(files)
                if verbose:
                    print(self.client_csv_data[self.client].shape)
                    print('loaded dataframe shape')
        else:
            if verbose:
                print('No current csv log file')
            self.current_file_exists = False
            self.new_file = True
            self.client_csv_file_address[self.client] = (self.path_prefix + self.client + '_' +
                                                         self.year + '_' + self.month + '_' + self.day + '_silver_log.csv')
            self.client_csv_data[self.client] = pd.DataFrame()
            self.current_file_exists = True
        if self.new_file:
            if verbose:
                print(self.current_file_exists, self.new_file)

    def append_and_merge_initial_csv_data(self):
        try:
            new_data = pd.DataFrame(self.client_json_data)
            print(new_data.shape)
            print('shape of incoming data')
            self.new_data_flag = True
        except Exception:
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
            data_file = self.client_csv_data[self.client]
            self.updated_df[self.client] = pd.concat([data_file, new_data]).drop_duplicates()
            print(data_file.shape)
            print('current data file shape')
            print(self.updated_df[self.client].shape)
            print('updated data file shape')
        else:
            print('no file')  # Generate new dataframe object for concat event
        #        pdb.set_trace()

    def save_local_client_file(self):
        if not self.new_data_flag:
            print('bad file')
        else:
            self.updated_df[self.client].to_csv(self.client_csv_file_address[self.client], index=False)

    def save_all_files_on_exit(self):
        for client, file_locations in self.client_csv_file_address.items():
            self.updated_df[client].to_csv(file_locations, index=False)  # Save over previous DF with concat version
