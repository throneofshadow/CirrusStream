"""

@Author: Brett Nelson, Yousolar Engineering

@version: 1.0

@description: This class is used to parse and construct data structures streamed on AWS from
the PowerCon firehose to local EC2 or EKS instances. Currently, both reading from local files and accepting
streamed data are both acceptable methods of interacting with DatabaseQuery.

@notes: Currently supported databases are simple pandas data structures, parquet, and duckdb.


"""
import pandas as pd
import pdb
import warnings
import pyarrow

pd.options.mode.chained_assignment = None  # default='warn'
warnings.simplefilter(action='ignore', category=FutureWarning)  # disable pandas FutureWarnings
pd.options.mode.string_storage = "pyarrow"  # use pyarrow as string storage backend to improve speed


def read_csv_file_pandas(file_path) -> pd.DataFrame:
    """ Function intended to read a csv file using pandas.
        @type file_path: str
        @param file_path : The address of the file to be read.
        @return: pd.DataFrame        @returns: pd.DataFrame
    """
    return pd.read_csv(file_path, engine='pyarrow')


class DatabaseQuery:
    """
    Class methods used to import structured data from the Powercon data firehose into multiple types of database files.
    Currently supported databases are simple pandas data structures and DuckDB.
    To obtain a properly constructed database in pandas, use the 'construct_database_pandas' function.
    To obtain a properly constructed database in duckdb, use the 'construct_database_duckdb' function.
    To obtain a database in parquet format, use the 'construct_database_parquet' function.
    To obtain a database spanning multiple days for a single client, use the argument
    'date = ['YYYY-MM-DD', 'YYYY-MM-DD'..]'.
    Multi-client database support is documented for several use cases.

    @notes: Currently supported databases are simple pandas data structures, parquet.



    """

    def __init__(self, data_file):

        self.is_query = False
        self.is_file = False
        self.record_type_key_value = {
            "PCON_BOOT_STATUS": 30,  # pcon_common.h               Pcon_Status_t
            "PCON_NODE_SUMMARY": 31,  # pcon_main.h                 Blh_Node_Id_List_t
            "PCON_CONTROL_STATUS": 32,  # pcon_control_loop.h         Control_Loop_Status_t
            "BUSBAR_RECORD": 33,  # pcon_busbar_transactions.h  Busbar_Record_t
            "EXTEND_RECORD": 34,  # pcon_extend_transactions.h  Extend_Record_t
            "FORCE_RECORD": 35,  # pcon_force_transactions.h   Force_Record_t
            "STEPIN_RECORD": 36,  # pcon_step_transactions.h    Step_Record_t
            "PRODUCTION_RECORD": 37,  # pcon_step_transactions.h    Production_Record_t
            "TWIN_OPAL_RECORD": 38,  # pcon_twin_transactions.h    Twin_Record_t
            "TWIN_STORAGE_RECORD": 39,  # pcon_twin_transactions.h    Storage_Record_t
            "TEST_MESSAGE": 40  # Traffic_Test_Message_t
        }
        self.sources_key_value = {

        }
        self.data_file = data_file
        self.ex_df = pd.read_csv(self.data_file)
        self.ex_df = self.ex_df.convert_dtypes(convert_boolean=False,
                                               convert_floating=False,
                                               convert_integer=False,
                                               )
        # Individual records possible.
        # Multiple units possible per record (see sources key)
        self.twin_records, self.twin_storage_records, self.solar_production_records = None, None, None
        self.inverter_record, self.step_record, self.control_status, self.rectifier_control = None, None, None, None
        self.boot_status, self.node_summary, self.busbar_record = None, None, None
        self.set_up_static_dataframes_for_units()

    ### We need to divide real numbers by % 100.0
    def refresh_database(self):
        # Re-load data
        self.ex_df = pd.read_csv(self.data_file)
        self.twin_records, self.twin_storage_records, self.solar_production_records = None, None, None
        self.inverter_record, self.step_record, self.control_status, self.rectifier_control = None, None, None, None
        self.boot_status, self.node_summary, self.busbar_record = None, None, None
        self.set_up_static_dataframes_for_units()

    def return_record_type_dataframes(self, record_number=int):
        return self.ex_df.loc[self.ex_df['record_type'] == record_number]
        # self.sql_style = False

    def set_up_static_dataframes_for_units(self):
        """
        Function used to parse record ID's necessary for splitting data into proper streams.
        Data is parsed in down-stream functions for dashboarding and analytics.
        Data structures are pandas DataFrames, containing individualized records
        """
        self.twin_records = self.return_record_type_dataframes(38)
        time = pd.to_datetime(self.twin_records['epoch_time'], unit='s').to_list()
        self.twin_records.loc[:, ('timestamp')] = time  # We can pivot each unique record around the timestamp
        self.twin_records = self.twin_records.dropna(axis=1, how='all')

        self.busbar_record = self.return_record_type_dataframes(self.record_type_key_value['BUSBAR_RECORD'])
        busbar_time = pd.to_datetime(self.busbar_record['epoch_time'], unit='s').to_list()
        self.busbar_record.loc[:, ('timestamp')] = busbar_time
        self.busbar_record = self.busbar_record.dropna(axis=1, how='all')

        self.step_record = self.return_record_type_dataframes(self.record_type_key_value['STEPIN_RECORD'])
        step_time = pd.to_datetime(self.step_record['epoch_time'], unit='s').to_list()
        self.step_record.loc[:, ('timestamp')] = step_time
        self.step_record = self.step_record.dropna(axis=1, how='all')

        self.solar_production_records = self.return_record_type_dataframes(
            self.record_type_key_value["PRODUCTION_RECORD"])
        solar_prod_time = pd.to_datetime(self.solar_production_records['epoch_time'], unit='s').to_list()
        self.solar_production_records.loc[:, ('timestamp')] = solar_prod_time
        self.solar_production_records = self.solar_production_records.dropna(axis=1, how='all')

        self.control_status = self.return_record_type_dataframes(
            self.record_type_key_value['PCON_CONTROL_STATUS'])  # 32
        control_time = pd.to_datetime(self.control_status['epoch_time'], unit='s').to_list()
        self.control_status.loc[:, ('timestamp')] = control_time
        self.control_status = self.control_status.dropna(axis=1, how='all')

        self.inverter_record = self.return_record_type_dataframes(self.record_type_key_value['FORCE_RECORD']).dropna(
            axis=1, how='all')
        inverter_time = pd.to_datetime(self.inverter_record['epoch_time'], unit='s').to_list()
        self.inverter_record.loc[:, ('timestamp')] = inverter_time
        self.inverter_record = self.inverter_record.dropna(axis=1, how='all')

        self.rectifier_control = self.return_record_type_dataframes(self.record_type_key_value['EXTEND_RECORD'])
        rectifier_time = pd.to_datetime(self.rectifier_control['epoch_time'], unit='s').to_list()
        self.rectifier_control.loc[:, ('timestamp')] = rectifier_time
        self.rectifier_control = self.rectifier_control.dropna(axis=1, how='all')

        self.node_summary = self.return_record_type_dataframes(self.record_type_key_value['PCON_NODE_SUMMARY'])
        node_time = pd.to_datetime(self.node_summary['epoch_time'], unit='s').to_list()
        self.node_summary.loc[:, ('timestamp')] = node_time
        self.node_summary = self.node_summary.dropna(axis=1, how='all')

        self.boot_status = self.return_record_type_dataframes(self.record_type_key_value['PCON_BOOT_STATUS'])
        boot_time = pd.to_datetime(self.boot_status['epoch_time'], unit='s').to_list()
        self.boot_status.loc[: , ('timestamp')] = boot_time
        self.boot_status = self.boot_status.dropna(axis=1, how='all')

        self.twin_storage_records = self.return_record_type_dataframes(
            self.record_type_key_value['TWIN_STORAGE_RECORD'])
        storage_time = pd.to_datetime(self.twin_storage_records['epoch_time'], unit='s').to_list()
        self.twin_storage_records.loc[:, ('timestamp')] = storage_time
        self.twin_storage_records = self.twin_storage_records.dropna(axis=1, how='all')

if __name__ == "__main__":
    data_file = input("Data file to be monitored")
    DB = DatabaseQuery(data_file)
    pdb.set_trace()
