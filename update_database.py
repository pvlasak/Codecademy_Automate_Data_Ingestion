import json
import pandas as pd


def df_students_updates(df):
    df_new = df.copy(deep=True)
    df_new['job_id'].fillna(0, inplace=True)
    median_time_spent = round(df_new['time_spent_hrs'].median(), 0)
    df_new['time_spent_hrs'].fillna(median_time_spent, inplace=True)
    df_new['current_career_path_id'].fillna(0, inplace=True)
    median_num_course_taken = round(df_new['num_course_taken'].median(), 0)
    df_new['num_course_taken'].fillna(median_num_course_taken, inplace=True)
    df_new['contact_info'] = df_new.contact_info.apply(lambda x: json.loads(x))
    df_new['address'] = df_new.contact_info.map(lambda q: q['mailing_address'])
    df_new['email'] = df_new.contact_info.map(lambda q: q['email'])
    df_new['state'] = df_new.address.map(lambda q: q.split(',')[2])
    df_new.job_id = pd.to_numeric(df_new.job_id)
    df_new.num_course_taken = pd.to_numeric(df_new.num_course_taken)
    df_new.current_career_path_id = pd.to_numeric(df_new.current_career_path_id)
    df_new.time_spent_hrs = pd.to_numeric(df_new.time_spent_hrs)
    df_new.job_id = df_new.job_id.astype(int)
    df_new.num_course_taken = df_new.num_course_taken.astype(int)
    df_new.current_career_path_id = df_new.current_career_path_id.astype(int)
    df_new.drop(labels='contact_info', axis=1, inplace=True)
    df_new.rename(mapper={'dob': 'date_of_birth', 'current_career_path_id': 'career_path_id'}, axis=1,
                  inplace=True)
    yield df_new


def df_courses_updates(df):
    new_row = {'career_path_id': [0], 'career_path_name': ['unknown'], 'hours_to_complete': [0]}
    df_new_row = pd.DataFrame(data=new_row)
    df_new = pd.concat([df_new_row, df], axis=0).reset_index().drop(labels='index', axis=1)
    yield df_new


def df_student_jobs_updates(df):
    df_new = df.drop_duplicates()
    yield df_new


def df_generator(table_names, df_dict):
    for tbl in table_names:
        if 'cademycode_students' in tbl:
            yield from df_students_updates(df_dict['cademycode_students'])
        elif 'cademycode_courses' in tbl:
            yield from df_courses_updates(df_dict['cademycode_courses'])
        else:
            yield from df_student_jobs_updates(df_dict['cademycode_student_jobs'])


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
