from automated_data_ingestion import *
from check_functions import *

orig_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode.db"
new_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode_updated.db"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)


class DatabaseTestsClass(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logger.debug('Starting unit tests session after database update...')

    def test_db_length(self):
        orig_info = check_db(orig_db_path)
        new_info = check_db(new_db_path)
        message = 'Original and updated database have not same number of rows.'
        self.assertTrue(compare_db_length(orig_info, new_info), message)

    def test_db_row_sequence(self):
        orig_info = check_db(orig_db_path)
        new_info = check_db(new_db_path)
        message = 'Original and updated database have different row sequence!'
        self.assertTrue(compare_row_sequence(orig_info, new_info), message)

    def test_orig_db_tables(self):
        db = Database('original', orig_db_path)
        db.get_connection()
        db.get_cursor()
        tables = db.get_tables()
        message = "Number of tables in original database has been changed."
        self.assertEqual(len(tables), 3, message)

    @unittest.skipUnless(os.path.isfile(new_db_path), "Database file does not exist.")
    def test_new_db_tables(self):
        db = Database('new', new_db_path)
        db.get_connection()
        db.get_cursor()
        tables = db.get_tables()
        message = "Number of tables in the new database has been changed."
        self.assertEqual(len(tables), 3, message)

    def test_orig_number_of_rows(self):
        info = check_db(orig_db_path)
        row_count = [5000, 10, 13]
        for count, table in enumerate(info):
            message = f"Tables in the original database have different number of rows than expected."
            with self.subTest(table):
                self.assertEqual(table['number_of_rows'], row_count[count], message)

    @unittest.skipUnless(os.path.isfile(new_db_path), "Database file does not exist.")
    def test_new_number_of_rows(self):
        info = check_db(new_db_path)
        row_count = [5000, 11, 10]
        for count, table in enumerate(info):
            message = f"Tables in the new database have different number of rows than expected."
            with self.subTest(table):
                self.assertEqual(table['number_of_rows'], row_count[count], message)

    def test_orig_number_of_cols(self):
        info = check_db(orig_db_path)
        cols_count = [9, 3, 3]
        for count, table in enumerate(info):
            message = f"Tables in the original database have different number of columns than expected."
            with self.subTest(table):
                self.assertEqual(table['number_of_columns'], cols_count[count], message)

    @unittest.skipUnless(os.path.isfile(new_db_path), "Database file does not exist.")
    def test_new_number_of_cols(self):
        info = check_db(new_db_path)
        cols_count = [11, 3, 3]
        for count, table in enumerate(info):
            message = f"Tables in the new database have different number of columns than expected."
            with self.subTest(table):
                self.longMessage = True
                self.assertEqual(table['number_of_columns'], cols_count[count], message)

    @unittest.skipUnless(os.path.isfile(new_db_path), "Database file does not exist.")
    def test_nan_values(self):
        db = Database('new', new_db_path)
        db.get_connection()
        db.get_cursor()
        tables = db.get_tables()
        for table in tables:
            df = Dataframe(table, db.con)
            df.get_data()
            nan_count = df.get_nan_value_count()
            with self.subTest(table):
                self.assertEqual(nan_count, 0)

    @unittest.skipUnless(os.path.isfile(new_db_path), "Database file does not exist.")
    def test_new_path(self):
        db = Database('new', new_db_path)
        self.assertTrue(db.check_path())

    @unittest.skipUnless(os.path.isfile(new_db_path), "Database file does not exist.")
    def test_updates_needed(self):
        orig_info = check_path_and_load_data("original", orig_db_path)
        new_info = check_path_and_load_data("new", new_db_path)
        new_st, del_st = check_students(orig_info, new_info)
        for update in [new_st, del_st]:
            message = f"Table still includes NAN values."
            with self.subTest(update):
                self.assertEqual(len(update), 0, message)

    @unittest.skipUnless(os.path.isfile(new_db_path), "Database file does not exist.")
    def test_new_file_size(self):
        db = Database('new', new_db_path)
        message = "File has zero size."
        self.assertGreater(db.size, 0, message)

    def test_rows_csv(self):
        with FinalCSVReader("./subscriber-pipeline-starter-kit/dev/combined_file.csv") as csv_mapper:
            n_rows = len(tuple(csv_mapper))
        n_db_rows = check_db(orig_db_path)[0]['number_of_rows']
        self.assertEqual(n_rows, n_db_rows)

    def test_cols_csv(self):
        with FinalCSVReader("./subscriber-pipeline-starter-kit/dev/combined_file.csv") as mapper:
            n_cols = len(tuple(mapper)[0])
        self.assertEqual(n_cols, 13)

    @classmethod
    def tearDownClass(cls):
        logger.debug('Terminating unit tests session.')


if __name__ == '__main__':
    unittest.main()
