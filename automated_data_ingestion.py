from update_database import *
from check_functions import *
from objects import *
import logging
import sys
import unittest

orig_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode.db"
new_db_path = "./subscriber-pipeline-starter-kit/dev/cademycode_updated.db"
db_folder = "./subscriber-pipeline-starter-kit/dev/"


def check_path_and_load_data(label, path_db):
    db = Database(label, path_db)
    db_path_check = db.check_path()
    log_check_db_path(db_path_check)
    db_info = check_db(path_db)
    return db_info


def download_db(db_label, path):
    pipeline = DataTransfer(db_label, path)
    dfs = pipeline.download_from_sql()
    new_tb_names = pipeline.get_new_table_names()
    pipeline.commit_and_close()
    return dfs, new_tb_names


def update_data(dfs, new_tb_names):
    new_df_gn = df_generator(new_tb_names, dfs)
    new_dfs = {new_tb_names[i]: next(new_df_gn) for i in range(len(new_tb_names))}
    return new_dfs


def upload_to_db(target_db_label, db_path, df_to_upload, table_names_to_upload):
    pipeline = DataTransfer(target_db_label, db_path)
    col_data = [df_students_new_cols, df_courses_new_cols, df_student_jobs_new_cols]
    for i in range(len(df_to_upload)):
        pipeline.upload_to_sql(table_names_to_upload[i], col_data[i], df_to_upload)
    pipeline.commit_and_close()


if __name__ == '__main__':
    orig_db_info = check_path_and_load_data("original", orig_db_path)
    new_db_info = check_path_and_load_data("new", new_db_path)
    new_students, removed_students = check_students(orig_db_info, new_db_info)
    orig_dataframes, new_table_names = download_db("original", orig_db_path)
    new_dataframes = update_data(orig_dataframes, new_table_names)
    upload_to_db("new", new_db_path, new_dataframes, new_table_names)

    updated_dataframes, _ = download_db("new", new_db_path)
    df_merged = join_dfs(updated_dataframes)
    export_df(df_merged)
