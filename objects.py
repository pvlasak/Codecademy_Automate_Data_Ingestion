import sqlite3
import pandas as pd
import json

orig_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode.db"
new_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode_updated.db"
db_folder = "./subscriber-pipeline-starter-kit/dev/"


class SQLite:
    def __init__(self, file):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        return self.conn.cursor()

    def __exit__(self, *exc):
        self.conn.commit()
        self.conn.close()


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


class Dataframe:
    def __init__(self, table_name, conn):
        self.table_name = table_name
        self.conn = conn
        self.df = None

    def get_data(self):
        self.df = pd.read_sql_query(f"SELECT * FROM {self.table_name}", con=self.conn)
        return self.df


class DataTransfer(Database, Dataframe):
    def __init__(self, db_name, db_path):
        self.db = Database(db_name, db_path)
        self.connection = self.db.get_connection()
        self.cursor = self.db.get_cursor()
        self.df_dict_down = {}
        self.df_dict_up = None

    def download_all_from_sql(self):
        self.table_names = self.db.get_tables()
        self.new_table_names = self.db.get_new_tables()
        for table in self.table_names:
            df = Dataframe(table, self.connection)
            self.df_dict_down[table] = df.get_data()
        return self.df_dict_down, self.new_table_names

    def upload_to_sql(self, table_name, column_data, df_dict_up):
        self.table_name = table_name
        self.column_data = column_data
        self.df_dict_up = df_dict_up
        self.db.set_table(self.table_name, self.column_data)
        df = df_dict_up[self.table_name]
        df.to_sql(self.table_name, self.connection, if_exists='replace', index=False)

    def commit_and_close(self):
        self.connection.commit()
        self.connection.close()