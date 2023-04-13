from update_database import *
from unit_tests import *
from objects import *
import logging
import sys

orig_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode.db"
new_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode_updated.db"
db_folder = "./subscriber-pipeline-starter-kit/dev/"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)

if __name__ == '__main__':
    orig_db_path_check = Database("original", orig_db_path).check_path()
    log_check_db_path(orig_db_path_check)
    orig_db_info = check_db(orig_db_path)
    new_db_path_check = Database("new", new_db_path).check_path()
    log_check_db_path(new_db_path_check)
    new_db_info = check_db(new_db_path)
    new_students, removed_students = check_students(orig_db_info, new_db_info)
    download_pipeline = DataTransfer("original", orig_db_path)
    orig_dataframes = download_pipeline.download_from_sql()
    new_table_names = download_pipeline.get_new_table_names()
    new_df_gen = df_generator(new_table_names, orig_dataframes)
    new_dataframes = {new_table_names[i]: next(new_df_gen) for i in range(len(new_table_names))}
    upload_pipeline = DataTransfer("new", new_db_path)
    column_data = [df_students_new_cols, df_courses_new_cols, df_student_jobs_new_cols]
    for i in range(len(new_dataframes)):
        upload_pipeline.upload_to_sql(new_table_names[i], column_data[i], new_dataframes)
    download_pipeline.commit_and_close()
    upload_pipeline.commit_and_close()

    upd_download = DataTransfer("new", new_db_path)
    updated_dataframes = upd_download.download_from_sql()
    join_dfs_and_export(updated_dataframes)
    upd_download.commit_and_close()
    new_db_after_update = check_db(new_db_path)


    class DatabaseTestsClass(unittest.TestCase):

        @classmethod
        def setUpClass(cls):
            logger.debug('Starting unit tests session after database update...')

        def test_db_length(self):
            logger.debug('Testing database length after update...')
            message = 'Original and updated database have not same number of rows.'
            self.assertTrue(compare_db_length(orig_db_info, new_db_after_update), message)

        def test_db_row_sequence(self):
            logger.debug('Testing database row sequence after update...')
            message = 'Original and updated database have different row sequence!'
            self.assertTrue(compare_row_sequence(orig_db_info, new_db_after_update), message)

        @classmethod
        def tearDownClass(cls):
            logger.debug('Terminating unit tests session.')

    unittest.main()
