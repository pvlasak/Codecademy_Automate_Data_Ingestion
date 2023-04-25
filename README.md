## Project Description
This project is focused on automated data ingestion into target SQLite Database.
Original database data have been inspected within Jupyter Notebook with regard to missing values, data types, naming, errors, duplicities etc. 
All findings gained from Jupyter Notebook were later used to create a Python script dealing with automated update of database. 
Clean tabular data in updated database are subsequently merged in a single dataframe and exported as a single CSV file. 
The whole process of data transformation is tracked by writing logging messages into log files.

## Installation of Python Packages
Python packages which are referenced in the main python file are listed in requirements.txt. 
Before their installation is recommended to set up virtual environment and to define Python interpreter. 

Python packages can be installed as follows:

pip install -r requirements.txt

## File Description
`automated_data_ingestion.py:` main python file\
	- reads data from original source\
	- checks for new and removed lines\
	- establishing data transfer pipeline for download from original database to Pandas dataframe\
	- update dataframe data\
	- establishing data transfer pipeline for upload to an updated database\
	- merging clean tabular data\
	- exporting final CSV

`check_functions.py:`\
	- context manager for connecting to sqlite database via python \
	- function creating a named tuple container \
	- class TableInfo collecting the information about the database \
	- function checking for new and removed lines in original database \
	- function checking for equal number of rows for table containing student records \
	- function checking the row sequence for original and new table containing student records\
	- logging module is used. 

`objects.py:` \
	- class Database incl. methods\
	- class Dataframe incl. methods\
	- class DataTransfer incl. methods\
 	- class FinalCSVReader incl. methods

`unit_tests.py:`\
 	- unit tests checking the process of database update \
	- can be executed individually after the database update. \
		- testing if original and new database has same number of rows for student table. \
		- testing if the row sequence is same in both databases for table containing student data. \
		- testing the total number of tables in database\
		- testing the number of rows and columns in both databases. \
		- testing the occurence of nan values in an updated database\
		- testing if the database file path exists. \
		- testing if updated database file has size > 0 bytes. \
		- testing if there are any new or removed lines in original database. \
		- testing the number of rows and columns in csv file. \
`changes.log:`
	- contains the info and debug logs regarding update\
`check_errors.log:`
	- contains the warning and error logs regarding update. \
## Initialization of Automated Data Ingestion

Database update can be initialized in terminal by starting the command: \
"python3 ./automated_data_ingestion.py"

Update process can be tracked in files `changes.log` and `check_errors.log` \

Output files are following:\
	a] "./subscriber-pipeline-starter-kit/dev/cademycode_updated.db"\
		clean database \
	b] "./subscriber-pipeline-starter-kit/dev/combined_file.csv"\
		merged tabular data from clean database

To protect the process of database update the unit testing sequence can be started anytime:\
	"python3 ./unit_tests.py -v"