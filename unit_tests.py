import unittest
import json


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


def get_db_data_len(db_data, label):
    print(f'Calculating the number of rows for {label.lower()} database...')
    print(f'{label.title()} database has {len(db_data)} rows.')
    return len(db_data)


def get_orig_emails(orig_data):
    #print('Retrieving row sequence from original database...')
    iterator = DatabaseIterator(orig_data)
    orig_emails = []
    for line in iterator:
        orig_emails.append(json.loads(line.contact_info)['email'])
    return orig_emails


def get_new_emails(new_data):
    #print('Retrieving row sequence from new database...')
    iterator = DatabaseIterator(new_data)
    new_emails = []
    for line in iterator:
        new_emails.append(line.email)
    return new_emails

