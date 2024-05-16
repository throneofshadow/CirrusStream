"""

@Author: Brett Nelson, Yousolar Engineering

@version: 1.0

@description: This class is used to parse and construct data structures streamed on AWS from
the PowerCon firehose to local EC2 or EKS instances. Currently, both reading from local files and accepting
streamed data are both acceptable methods of interacting with DatabaseFormatter.
DatabaseFormatter is used to quickly scope different variables without having to load into traditional database options.
This particular class is not intended for production environments and is only
intended for testing and engineering purposes.

@notes: Currently supported databases are simple pandas data structures, parquet, and duckdb.


"""
import pandas as pd
import pdb
import warnings
import pyarrow
pd.options.mode.string_storage = "pyarrow"  # use pyarrow as string storage backend to improve speed


def read_csv_file_pandas(file_path) -> pd.DataFrame:
    """ Function intended to read a csv file using pandas.
        @type file_path: str
        @param file_path : The address of the file to be read.
        @return: pd.DataFrame        @returns: pd.DataFrame
    """
    return pd.read_csv(file_path, engine='pyarrow')


class DatabaseFormatter:
    """
    Class methods used to import structured data from the Powercon data firehose into multiple types of database files.
    A user provides the file address of a csv file containing row-structured data from the Powercon firehose.
    The class provides methods to convert the data into different types of database files, data types,
    and unit-specific dataframes.
    Record-specific data types are defined in the 'record_type_key_value' dictionary.
    Record specific dataframes are created using the 'set_up_static_dataframe' function.
    Pyarrow is used to read, write, and serialize data types, including string-type.
    Currently supported simple output database files are simple pandas data structures, SQL-style arrow files, and duckdb.
    Multi-client database support is documented for several use cases.

    @notes: Currently supported databases are simple pandas dataframes into csv files. For more complex queries, please
    use the CirrusAnalytics platform.

    """

    def __init__(self, csv_unmodified_data_file_address):
        self.is_query = False
        self.is_file = False
        self.parquet_file = None
        self.duck_file = None
        self.structured_pandas_file = None
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
        self.data_file = csv_unmodified_data_file_address
        self.simple_pd_dataframe = pd.read_csv(self.data_file)
        self.simple_pd_dataframe = self.simple_pd_dataframe.convert_dtypes(convert_boolean=False,
                                                                           convert_floating=False,
                                                                           convert_integer=False,
                                                                           )
        # Individual records possible.
        # Multiple units possible per record (see sources key)
        self.twin_records, self.twin_storage_records, self.solar_production_records = None, None, None
        self.inverter_record, self.step_record, self.control_status, self.rectifier_control = None, None, None, None
        self.boot_status, self.node_summary, self.bus_bar_record = None, None, None
        self.set_up_static_dataframes_for_physical_units()

    def refresh_database(self) -> None:
        """
        Re-load data
        """
        # Re-load data
        self.simple_pd_dataframe = pd.read_csv(self.data_file)
        self.twin_records, self.twin_storage_records, self.solar_production_records = None, None, None
        self.inverter_record, self.step_record, self.control_status, self.rectifier_control = None, None, None, None
        self.boot_status, self.node_summary, self.bus_bar_record = None, None, None
        self.set_up_static_dataframes_for_physical_units()

    def return_record_type_dataframes(self, record_number=int) -> pd.DataFrame:
        """
        Returns dataframes for a specific record type based on the record number provided.

        :param record_number: The record number to filter the dataframes.
        :type record_number: int
        :return: DataFrame
        """
        return self.simple_pd_dataframe.loc[self.simple_pd_dataframe['record_type'] == record_number]

    def set_up_static_dataframes_for_physical_units(self) -> None:
        """
        Function used to set up static dataframes for physical units by parsing record IDs.
        Data is retrieved based on record type key values and stored in pandas DataFrames.
        Each DataFrame contains specific records related to physical units like twin, bus bar, step, solar production, control status, inverter, rectifier control, node summary, boot status, and twin storage records.
        """
        self.twin_records = self.return_record_type_dataframes(self.record_type_key_value['TWIN_OPAL_RECORD'])
        time = pd.to_datetime(self.twin_records['epoch_time'], unit='s')
        self.twin_records = pd.concat([self.twin_records, time], axis=1).reset_index(drop=True)
        self.twin_records = self.twin_records.dropna(axis=1, how='all')

        self.bus_bar_record = self.return_record_type_dataframes(self.record_type_key_value['BUSBAR_RECORD'])
        bus_bar_time = pd.to_datetime(self.bus_bar_record['epoch_time'], unit='s')
        self.bus_bar_record = pd.concat([self.bus_bar_record, bus_bar_time], axis=1).reset_index(drop=True)
        self.bus_bar_record = self.bus_bar_record.dropna(axis=1, how='all')

        self.step_record = self.return_record_type_dataframes(self.record_type_key_value['STEPIN_RECORD'])
        step_time = pd.to_datetime(self.step_record['epoch_time'], unit='s')
        self.step_record = pd.concat([self.step_record, step_time], axis=1).reset_index(drop=True)
        self.step_record = self.step_record.dropna(axis=1, how='all')

        self.solar_production_records = self.return_record_type_dataframes(
            self.record_type_key_value["PRODUCTION_RECORD"])
        solar_prod_time = pd.to_datetime(self.solar_production_records['epoch_time'], unit='s')
        self.solar_production_records = pd.concat([self.solar_production_records, solar_prod_time],
                                                  axis=1).reset_index(drop=True)
        self.solar_production_records = self.solar_production_records.dropna(axis=1, how='all')

        self.control_status = self.return_record_type_dataframes(
            self.record_type_key_value['PCON_CONTROL_STATUS'])  # 32
        control_time = pd.to_datetime(self.control_status['epoch_time'], unit='s')
        self.control_status = pd.concat([self.control_status, control_time], axis=1).reset_index(drop=True)
        self.control_status = self.control_status.dropna(axis=1, how='all')

        self.inverter_record = self.return_record_type_dataframes(self.record_type_key_value['FORCE_RECORD']).dropna(
            axis=1, how='all')
        inverter_time = pd.to_datetime(self.inverter_record['epoch_time'], unit='s')
        self.inverter_record = pd.concat([self.inverter_record, inverter_time], axis=1).reset_index(drop=True)
        self.inverter_record = self.inverter_record.dropna(axis=1, how='all')

        self.rectifier_control = self.return_record_type_dataframes(self.record_type_key_value['EXTEND_RECORD'])
        rectifier_time = pd.to_datetime(self.rectifier_control['epoch_time'], unit='s')
        self.rectifier_control = pd.concat([self.rectifier_control, rectifier_time],
                                           axis=1).reset_index(drop=True)
        self.rectifier_control = self.rectifier_control.dropna(axis=1, how='all')

        self.node_summary = self.return_record_type_dataframes(self.record_type_key_value['PCON_NODE_SUMMARY'])
        node_time = pd.to_datetime(self.node_summary['epoch_time'], unit='s')
        self.node_summary = pd.concat([self.node_summary, node_time],
                                      axis=1).reset_index(drop=True)
        self.node_summary = self.node_summary.dropna(axis=1, how='all')

        self.boot_status = self.return_record_type_dataframes(self.record_type_key_value['PCON_BOOT_STATUS'])
        boot_time = pd.to_datetime(self.boot_status['epoch_time'], unit='s')
        self.boot_status = pd.concat([self.boot_status, boot_time],
                                     axis=1).reset_index(drop=True)
        self.boot_status = self.boot_status.dropna(axis=1, how='all')

        self.twin_storage_records = self.return_record_type_dataframes(
            self.record_type_key_value['TWIN_STORAGE_RECORD'])
        storage_time = pd.to_datetime(self.twin_storage_records['epoch_time'], unit='s')
        self.twin_storage_records = pd.concat([self.twin_storage_records, storage_time],
                                              axis=1).reset_index(drop=True)
        self.twin_storage_records = self.twin_storage_records.dropna(axis=1, how='all')


if __name__ == "__main__":
    data_file = '/data/test_2024_05_16_silver_log.csv'
    #data_file = input("Data file to be monitored")
    DB = DatabaseFormatter(data_file)
    pdb.set_trace()
