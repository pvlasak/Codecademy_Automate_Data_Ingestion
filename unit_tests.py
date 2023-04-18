from objects import Database
import unittest
orig_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode.db"


class DatabaseTestsClass(unittest.TestCase):

    def test_orig_db_tables(self):
        db = Database('original', orig_db_path)
        db.get_connection()
        db.get_cursor()
        tables = db.get_tables()
        message = "Number of tables in original database has been changed."
        self.assertEqual(len(tables), 3, message)

unittest.main()