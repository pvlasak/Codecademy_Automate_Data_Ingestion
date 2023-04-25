## Project Description
This project is focused on automated data ingestion into target SQLite Database.
Original database data have been inspected within Jupyter Notebook and missing values, data types, naming, errors, duplicities etc. have been identified. 
All findings gained from Jupyter Notebook were used to create a Python script dealing with automated update of database. 
Clean tabular data in updated database are subsequently merged and resulting table is exported as a single CSV file. 
The whole process of data transformation is tracked by writing messages into log files.

## Installation of Python Packages


## File Description
`automated_data_ingestion.py:` main python file 
	- reads data from original source
	- checks for new and removed lines
	- establishing data trasfer for download from original database to Pandas dataframe
	- update dataframe data
	- establishing data trasfer for upload to an updated database
	- merging clean tabular data
	- exporting final CSV

`check_functions.py:`
	- context manager for connecting to sqlite database via python 
	- class TableInfo collecting the information about the database
	- function checking for new and removed lines
	- function checking for equal number of rows for each table
	- function checking the row sequence for original and new table containing information about students.
	- logging module is used. 

`objects.py:` 
	- class Database incl. methods
	- class Dataframe incl. methods
	- class DataTransfer incl. methods
 	- class FinalCSVReader incl. methods

`unit_tests.py:`
 	- unit tests checking the process of database update
	- can be executed individually after the database update. 
	- testing if original and new database has same number of rows for student table. 
	- testing if the row sequence is same in both databases for table containing student data. 
	- testing the total number of tables in database
	- testing the number of rows and columns in both databases. 
	- testing the occurence of nan values in an updated database
	- testing if the database file path exists. 
	- testing if updated database file has size > 0 bytes.
	- testing if there are any new or removed lines in original database.
	- testing the number of rows and columns in csv file. 


`changes.log:`
	- contains the info and debug logs regarding update

`check_errors.log:`
	- contains the warning and error logs regarding update. 


## Initialization of Automated Data Ingestion
