import sqlite3
import pandas as pd
import logging
import os
from functools import reduce
import csv
from collections import namedtuple


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter1 = logging.Formatter("[%(asctime)s] {%(levelname)s} %(name)s: #%(lineno)d - %(message)s")

file_handler = logging.FileHandler("changes.log")
file_handler.setFormatter(formatter1)
logger.addHandler(file_handler)


class Database:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.con = None
        self.cur = None
        self.column_data = None
        self.tab_name = None
        self.tables = []
        self.table_names = []
        self.new_tables = []
        self.new_table_names = []
        if os.path.isfile(self.path) is False:
            self.size = 0
        else:
            self.size = os.path.getsize(self.path)

    def get_connection(self):
        self.con = sqlite3.connect(self.path)
        return self.con

    def get_cursor(self):
        self.cur = self.con.cursor
        return self.cur

    def get_tables(self):
        self.tables = self.cur().execute("""SELECT name FROM sqlite_master WHERE type='table';""").fetchall()
        self.table_names = [table[0] for table in self.tables]
        return self.table_names

    def get_new_tables(self):
        self.new_tables = self.cur().execute("""SELECT name FROM sqlite_master WHERE type='table';""").fetchall()
        self.new_table_names = [table[0] + '_new' for table in self.new_tables]
        return self.new_table_names

    def set_table(self, tab_name, column_data: dict):
        self.column_data = column_data
        self.tab_name = tab_name
        column_data_list = [str(key) + ' ' + str(value) for key, value in self.column_data.items()]
        column_data_string = ', '.join(column_data_list)
        self.cur().execute(f"""CREATE TABLE IF NOT EXISTS {self.tab_name} ({column_data_string})""")

    def delete_table(self, tab_name):
        self.tab_name = tab_name
        self.cur().execute(f"""DROP TABLE {tab_name}""")

    def commit_and_close(self):
        self.con.commit()
        self.con.close()

    def check_path(self):
        logger.log(logging.DEBUG, f'Checking if {self.name} database file exists...')
        if os.path.isfile(self.path) is True:
            return self.name, True
        else:
            return self.name, False

    def get_file_size(self):
        return os.path.getsize(self.path)


class Dataframe:
    def __init__(self, table_name, conn):
        self.table_name = table_name
        self.conn = conn
        self.df = None

    def get_data(self):
        self.df = pd.read_sql_query(f"SELECT * FROM {self.table_name}", con=self.conn)
        return self.df

    def get_nan_value_count(self):
        nan_count = reduce(lambda x, y: x+y, self.df.isna().sum().to_list())
        return nan_count


class DataTransfer(Database, Dataframe):
    def __init__(self, db_name, db_path):
        self.db = Database(db_name, db_path)
        self.connection = self.db.get_connection()
        self.cursor = self.db.get_cursor()
        self.df_dict_down = {}
        self.df_dict_up = None
        logger.info("Establishing data transfer pipeline between database and dataframe...")

    def download_from_sql(self):
        logger.info(f"Downloading tables from SQL Database: {self.db.path}...")
        self.table_names = self.db.get_tables()
        for table in self.table_names:
            df = Dataframe(table, self.connection)
            self.df_dict_down[table] = df.get_data()
        return self.df_dict_down

    def get_new_table_names(self):
        logger.info("Setting up new table names..")
        self.new_table_names = self.db.get_new_tables()
        return self.new_table_names

    def upload_to_sql(self, table_name, column_data, df_dict_up):
        self.table_name = table_name
        self.column_data = column_data
        self.df_dict_up = df_dict_up
        self.db.set_table(self.table_name, self.column_data)
        logger.info(f"Uploading table {self.table_name} to SQL Database: {self.db.path}..")
        df = df_dict_up[self.table_name]
        df.to_sql(self.table_name, self.connection, if_exists='replace', index=False)

    def commit_and_close(self):
        self.connection.commit()
        self.connection.close()
        logger.info(f"Closing connection to database {self.db.path}.")


class FinalCSVReader:
    def __init__(self, file):
        self.file = file
        self.opened_file = None
        self.mapper = None

    def __enter__(self):
        self.opened_file = open(self.file, newline='')
        reader = csv.reader(self.opened_file, delimiter=',')
        csv_tuple = namedtuple("csv_tuple", next(reader)[1:])
        self.mapper = map(lambda line: csv_tuple(int(line[1]), str(line[2]), str(line[3]),
                                                 str(line[4]), int(line[5]), float(line[6]),
                                                 str(line[7]), str(line[8]), str(line[9]),
                                                 str(line[10]), int(line[11]), str(line[12]),
                                                 int(line[13])), reader)
        return self.mapper

    def __exit__(self, *exc):
        self.opened_file.close()

