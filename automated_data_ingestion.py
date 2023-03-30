import sqlite3
import json

class SQLite():
    def __init__(self, file):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        return self.conn.cursor()

    def __exit__(self, *exc):
        self.conn.commit()
        self.conn.close()


class DifferentLength(Exception):
    def __init__(self, length1, length2):
        self.length1 = length1
        self.length2 = length2

    def __str__(self):
        return 'Length of original database: ' + str(self.length1) + ' is not same as length of new database' + str(self.length2)


class EmailIterator:
    def __init__(self, database):
        self.database = database

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index < len(self.database):
            email = json.loads(self.database[self.index][4])['email']
            self.index += 1
            return email
        else:
            raise StopIteration

class DatabaseCheck:
    def __init__(self, db1, db2):
        self.original_db = db1
        self.new_db = db2

    def compare_length(self):
        if len(self.original_db) == len(self.new_db):
            return 'Length of the original and new database is same!'
        else:
            raise DifferentLength(len(self.original_db), len(self.new_db))


if __name__ == '__main__':
    with SQLite("./subscriber-pipeline-starter-kit/dev/cademycode.db") as cur:
        original_db = cur.execute('''SELECT * FROM cademycode_students''').fetchall()

    with SQLite("./subscriber-pipeline-starter-kit/dev/cademycode_clean.db") as cur:
        new_db = cur.execute('''SELECT * FROM cademycode_students_new''').fetchall()

    check = DatabaseCheck(original_db, new_db)
    length_response = check.compare_length()
    print(length_response)

    email_list = []
    email_list_generator = EmailIterator(original_db)
    for item in email_list_generator:
        email_list.append(item)
        print(item)
    print(len(email_list))