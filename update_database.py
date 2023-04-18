import json
import pandas as pd
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter1 = logging.Formatter("[%(asctime)s] {%(levelname)s} %(name)s: #%(lineno)d - %(message)s")

file_handler = logging.FileHandler("changes.log")
file_handler.setFormatter(formatter1)
logger.addHandler(file_handler)


def df_students_updates(df):
    df_new = df.copy(deep=True)
    logger.debug("All NAN values in 'job_id' column are being replaced by 0 -> 'other' job category.")
    df_new['job_id'].fillna(0, inplace=True)
    logger.debug("Median for column 'time_spent_hrs' is being calculated.")
    median_time_spent = round(df_new['time_spent_hrs'].median(), 0)
    logger.debug("All NAN values in column 'time_spent_hrs' are filled by median value.")
    df_new['time_spent_hrs'].fillna(median_time_spent, inplace=True)
    logger.debug("All NAN values in column 'current_career_path_id' are filled 0.")
    df_new['current_career_path_id'].fillna(0, inplace=True)
    logger.debug("Median for column 'num_course_taken' is being calculated.")
    median_num_course_taken = round(df_new['num_course_taken'].median(), 0)
    logger.debug("All NAN values in column 'num_course_taken' are filled by median value.")
    df_new['num_course_taken'].fillna(median_num_course_taken, inplace=True)
    logger.debug("Values from 'contact_info' are retrieved and saved as the new"
                 "columns named as 'address', 'email' and 'state'")
    df_new['contact_info'] = df_new.contact_info.apply(lambda x: json.loads(x))
    df_new['address'] = df_new.contact_info.map(lambda q: q['mailing_address'])
    df_new['email'] = df_new.contact_info.map(lambda q: q['email'])
    df_new['state'] = df_new.address.map(lambda q: q.split(',')[2])
    logger.debug("Values in 'job_id' column are converted to numeric format.")
    df_new.job_id = pd.to_numeric(df_new.job_id)
    logger.debug("Values in 'num_course_taken' column are converted to numeric format.")
    df_new.num_course_taken = pd.to_numeric(df_new.num_course_taken)
    logger.debug("Values in 'current_career_path_id' column are converted to numeric format.")
    df_new.current_career_path_id = pd.to_numeric(df_new.current_career_path_id)
    logger.debug("Values in 'time_spent_hrs' column are converted to numeric format.")
    df_new.time_spent_hrs = pd.to_numeric(df_new.time_spent_hrs)
    logger.debug("Values in 'job_id' column are converted to integer data type.")
    df_new.job_id = df_new.job_id.astype(int)
    logger.debug("Values in 'num_course_taken' column are converted to integer data type.")
    df_new.num_course_taken = df_new.num_course_taken.astype(int)
    logger.debug("Values in 'current_career_path_id' column are converted to integer data type.")
    df_new.current_career_path_id = df_new.current_career_path_id.astype(int)
    logger.debug("Column 'contact_info' is dropped.")
    df_new.drop(labels='contact_info', axis=1, inplace=True)
    df_new.rename(mapper={'dob': 'date_of_birth', 'current_career_path_id': 'career_path_id'}, axis=1,
                  inplace=True)
    yield df_new


def df_courses_updates(df):
    logger.debug("New row 'career_path_id' = 0 is being created.")
    new_row = {'career_path_id': [0], 'career_path_name': ['unknown'], 'hours_to_complete': [0]}
    df_new_row = pd.DataFrame(data=new_row)
    df_new = pd.concat([df_new_row, df], axis=0).reset_index().drop(labels='index', axis=1)
    yield df_new


def df_student_jobs_updates(df):
    logger.debug("Duplicated rows in dataframe 'df_student_jobs' are dropped.")
    df_new = df.drop_duplicates()
    yield df_new


def df_generator(table_names, df_dict):
    logger.debug("Starting the process of data wrangling, cleaning, tidying.")
    for tbl in table_names:
        if 'cademycode_students' in tbl:
            yield from df_students_updates(df_dict['cademycode_students'])
        elif 'cademycode_courses' in tbl:
            yield from df_courses_updates(df_dict['cademycode_courses'])
        else:
            yield from df_student_jobs_updates(df_dict['cademycode_student_jobs'])


def join_dfs(new_df_dict):
    logger.debug("Clean dataframes are merged into single one.")
    df_merged = new_df_dict['cademycode_students_new'].merge(new_df_dict['cademycode_student_jobs_new'], how='left',
                                                             on='job_id')
    df_merged = df_merged.merge(new_df_dict['cademycode_courses_new'], how='left', on='career_path_id')
    df_merged.drop(labels=['job_id', 'career_path_id'], axis=1, inplace=True)
    logger.debug("Merged dataframe is being exported into CSV File.")
    return df_merged


def export_df(df):
    df.to_csv("./subscriber-pipeline-starter-kit/dev/combined_file.csv")

df_students_new_cols = {'uuid': 'INTEGER',
                        'name': 'VARCHAR',
                        'date_of_birth': 'VARCHAR',
                        'sex': 'TEXT',
                        'job_id': 'INTEGER',
                        'num_course_taken': 'INTEGER',
                        'career_path_id': 'INTEGER',
                        'time_spent_hours': 'VARCHAR',
                        'address': 'VARCHAR',
                        'email': 'VARCHAR',
                        'state': 'VARCHAR'
                        }

df_courses_new_cols = {'career_path_id': 'INTEGER',
                       'career_path_name': 'VARCHAR',
                       'hours_to_complete': 'INTEGER'}

df_student_jobs_new_cols = {'job_id': 'INTEGER',
                            'job_category': 'VARCHAR',
                            'avg_salary': 'VARCHAR'}
