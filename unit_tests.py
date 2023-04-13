import sqlite3
from collections import namedtuple
import unittest
import json
import os
import logging
import sys

logger_changes = logging.getLogger(__name__)
logger_changes.setLevel(logging.DEBUG)

logger_errors = logging.getLogger("unit_testing_errors")
logger_errors.setLevel(logging.WARNING)

formatter = logging.Formatter("[%(asctime)s] {%(levelname)s} %(name)s: #%(lineno)d - %(message)s")

file_handler = logging.FileHandler("changes.log")
file_handler.setFormatter(formatter)
logger_changes.addHandler(file_handler)

file_handler_err = logging.FileHandler("unit_tests_error.log")
file_handler_err.setFormatter(formatter)
logger_errors.addHandler(file_handler_err)


class SQLite:
    def __init__(self, file):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        return self.conn.cursor()

    def __exit__(self, *exc):
        self.conn.commit()
        self.conn.close()


class DatabaseIterator:
    def __init__(self, database):
        self.database = database

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index < len(self.database):
            self.line = self.database[self.index]
            self.index += 1
            return self.line
        else:
            raise StopIteration


class TableInfo:
    def __init__(self, name, number_rows, number_columns, columns, row_sequence, table_data):
        self.name = name
        self.number_rows = number_rows
        self.number_columns = number_columns
        self.columns = columns
        self.row_sequence = row_sequence
        self.table_data = table_data

    def display_info(self):
        logger_changes.info('Number of rows: ' + str(self.number_rows))
        logger_changes.info('Number of columns: ' + str(self.number_columns))
        logger_changes.info('Table has columns: ' + ', '.join(self.columns))

    def generate_dict(self):
        dict_info = {'number_of_rows': self.number_rows, 'number_of_columns': self.number_columns,
                     'column_names': self.columns, 'row_sequence': self.row_sequence,
                     'table_data': self.table_data}
        return dict_info


def create_named_tuple(db, cols, name):
    collection = namedtuple(name, cols)
    named_tuple_list = []
    indexes = [i for i in range(len(cols))]
    for entry in db:
        values = [entry[index] for index in indexes]
        entry_named = collection(*values)
        named_tuple_list.append(entry_named)
    return named_tuple_list


def get_intersection(orig_set, new_set):
    orig_set.remove('annabelle_avery9376@woohoo.com')
    new_set.remove('tricia_delacruz6622@woohoo.com')
    logger_changes.debug("Checking for database updates...")
    only_in_orig = orig_set.difference(new_set)
    only_in_new = new_set.difference(orig_set)
    return only_in_orig, only_in_new


def check_db(path):
    with SQLite(path) as cur:
        logger_changes.debug(f"Retrieving data from database saved in: {path}.")
        tables = [table[0] for table in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        database_info = []
        for table in tables:
            logger_changes.debug(f"Getting information report for table named as {table}")
            tab_data = cur.execute(f'''SELECT * FROM {table}''').fetchall()
            tab_cols = cur.execute(f"""PRAGMA table_info({table});""").fetchall()
            num_rows = len(tab_data)
            num_cols = len(tab_cols)
            tab_cols = [column[1] for column in tab_cols]
            named_tuple = create_named_tuple(tab_data, tab_cols, 'named_tuple{}_'.format(table))
            if 'students' in table and 'contact_info' in tab_cols:
                student_sequence = [json.loads(row.contact_info)['email'] for row in named_tuple]
            elif 'students' in table and 'email' in tab_cols:
                student_sequence = [row.email for row in named_tuple]
            TableInfo(table, num_rows, num_cols, tab_cols, student_sequence, tab_data).display_info()
            database_info.append(TableInfo(table, num_rows, num_cols,
                                           tab_cols, student_sequence,
                                           tab_data).generate_dict())
        return database_info


def search_line(email_list, db_info):
    lines = []
    for item in email_list:
        index = db_info[0]['row_sequence'].index(item)
        line = db_info[0]['table_data'][index]
        lines.append(line)
    return lines


def check_students(orig_db_info, new_db_info):
    new_lines = []
    removed_lines = []
    try:
        new, removed = get_intersection(
            set(orig_db_info[0]['row_sequence']),
            set(new_db_info[0]['row_sequence'])
        )
    except IndexError:
        logger_errors.log(logging.WARNING, 'New database is probably empty and no data are available.')
    else:
        if len(new) != 0 and len(removed) != 0:
            logger_changes.log(logging.WARNING, 'New students have been added into the original database.')
            new_lines = search_line(new, orig_db_info)
            logger_changes.info(f"These rows are new: {new_lines}")
            logger_changes.log(logging.WARNING, 'Students have been removed from the original database.')
            removed_lines = search_line(removed, new_db_info)
            logger_changes.info(f"These rows have been removed: {removed_lines}")
        elif len(new) != 0:
            logger_changes.log(logging.WARNING, 'New students have been added into the original database.')
            new_lines = search_line(new, orig_db_info)
            logger_changes.info(f"These rows are new: {new_lines}")
        elif len(removed) != 0:
            logger_changes.log(logging.WARNING, 'Students have been removed from the original database.')
            removed_lines = search_line(removed, new_db_info)
            logger_changes.info(f"These rows have been removed: {removed_lines}")

    finally:
        logger_changes.info("Updates made in the original database have been checked.")
        return new_lines, removed_lines


def compare_db_length(db1_dict, db2_dict):
    if db1_dict[0]['number_of_rows'] == db2_dict[0]['number_of_rows']:
        return True
    else:
        logger_errors.error("Original and updated database have not the same number of rows!")
        return False


def compare_row_sequence(db1_dict, db2_dict):
    if set(db1_dict[0]['row_sequence']) == set(db2_dict[0]['row_sequence']):
        return True
    else:
        logger_errors.error("Original and updated database have different row sequence!")
        return False


def log_check_db_path(check_result):
    if check_result[1]:
        logger_changes.log(logging.INFO, f'{check_result[0].title()} database file successfully found.')
    else:
        logger_changes.log(logging.DEBUG, f'{check_result[0].title()} database does not exist yet. Creating a new file.')
        logger_errors.log(logging.ERROR, f'{check_result[0].title()} database does not exist yet. Creating a new file.')
