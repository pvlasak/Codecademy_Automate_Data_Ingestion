import sqlite3
import json
from collections import namedtuple
import pandas as pd
from update_database import *
from unit_tests import *
from objects import *


def create_named_tuple(db, cols, name):
    collection = namedtuple(name, cols)
    named_tuple_list = []
    indexes = [i for i in range(len(cols))]
    for entry in db:
        values = [entry[index] for index in indexes]
        entry_named = collection(*values)
        named_tuple_list.append(entry_named)
    return named_tuple_list


if __name__ == '__main__':
    orig_db = Database("original", orig_db_path)
    orig_db_connection = orig_db.get_connection()
    orig_db_cursor = orig_db.get_cursor()
    orig_tables = orig_db.get_tables()
    print(orig_tables)
    orig_table_names = [table[0] for table in orig_tables]
    orig_df_gen = (Dataframe(name, orig_db_connection).get_data() for name in orig_table_names)
    orig_df_dict = {orig_table_names[i]: next(orig_df_gen) for i in range(len(orig_table_names))}

    new_table_names = [table + '_new' for table in orig_table_names]
    new_df_gen = df_generator(new_table_names, orig_df_dict)
    new_df_dict = {new_table_names[i]: next(new_df_gen) for i in range(len(new_table_names))}

    new_db = Database("new", new_db_path)
    new_db_connection = new_db.get_connection()
    new_db_cursor = new_db.get_cursor()

    for table in new_table_names:
        table_name = str(table)
        new_db.set_table(table_name, df_students_new_cols)
        new_df_dict[table_name].to_sql(table_name, new_db_connection, if_exists='replace', index=False)

    #    new_db_tables = new_db.get_tables()
    #    print(new_db_tables)
    #
    #    print(new_df_dict['cademycode_student_jobs_new'].head(20))
    #    print(new_df_dict['cademycode_courses_new'].head(20))
    #    print('--------------------------')
    #    print(new_db_cursor().execute("""SELECT * FROM cademycode_students_new""").fetchmany(20))

    orig_db.commit_and_close()
    new_db.commit_and_close()

    with SQLite(orig_db_path) as cur:
        orig_db_data = cur.execute('''SELECT * FROM cademycode_students''').fetchall()

    with SQLite(new_db_path) as cur:
        new_db_data = cur.execute('''SELECT * FROM cademycode_students_new''').fetchall()

    orig_df_col_names = list(orig_df_dict['cademycode_students'].columns)
    new_df_col_names = list(new_df_dict['cademycode_students_new'].columns)

    original_named_tuple = create_named_tuple(orig_db_data, orig_df_col_names, 'original_named_tuple')
    new_named_tuple = create_named_tuple(new_db_data, new_df_col_names, 'new_named_tuple')


    class DatabaseTestsClass(unittest.TestCase):

        @classmethod
        def setUpClass(cls):
            print('Starting unit tests session...')

        def test_db_length(self):
            orig_db_len = get_db_data_len(original_named_tuple, label='original')
            new_db_len = get_db_data_len(new_named_tuple, label='new')
            message = 'Original and updated database have not same number of rows.'
            self.assertEqual(orig_db_len, new_db_len, message)

        def test_row_sequence(self):
            orig_email_lst = get_orig_emails(original_named_tuple)
            new_email_lst = get_new_emails(new_named_tuple)
            message = 'Row sequence is not identical with original database.'
            self.assertListEqual(orig_email_lst, new_email_lst, message)

        def test_new_students(self):
            for student in original_named_tuple:
                message = (f'New students has been added into original database: {student}')
                with self.subTest(student):
                    self.assertIn(json.loads(student.contact_info)['email'], get_new_emails(new_named_tuple), message)

        @classmethod
        def tearDownClass(cls):
            print('Terminating unit tests session.')

    unittest.main()

