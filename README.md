# ssdb_manager

Package built by the Tishman Speyer Data Analytics Team to expedite interactions with SQL Server. Import tables to pandas DataFrames, or update/delete tables in your database with one line of Python. Particularly useful when traversing between multiple databases. 

This package builds upon functionality available in pandas, pyodbc, and sqlalchemy. 

## Installation
Using pip:
``` 
pip install ssdb_manager
```

## Functions

#### import_table

```sh
import_table(key:tuple, table_name:str, schema:str = 'dbo', custom_connection:bool = False, driver:str = 'ODBC Driver 17 for SQL Server', show_progress:bool = False)
```
Imports and returns a full table from the database.
**Arguments:**
  - **key**: Tuple in format (server, database).
  - **table_name**: Name of table to be imported from database. 
  - schema: Schema in which table exists in database. Default = 'dbo'.
  - custom_connection: True if user wants do define their own conn and engine variables. False otherwise. Default = False.
  - driver: Driver used by sqlalchemy to connect with database. Only used if custom_connection = False. Default = 'ODBC Driver 17 for SQL Server'.
  - show_progress: True for printed output describing updates made to database. Default = False.

#### create_table
```sh
create_table(key:tuple, table_name:str, df, schema:str = 'dbo', custom_connection:bool = False, driver:str = 'ODBC Driver 17 for SQL Server', show_progress:bool = False, **kwargs)
```
Creates a table using the passed pandas DataFrame in the database.
**Arguments:**
  - **key**: Tuple in format (server, database).
  - **table_name**: Name of table to be created in the database. 
  - **df**: Pandas DataFrame object that table will be created from and populated with.
  - schema: Schema in which table exists in database. Default = 'dbo'.
  - custom_connection: True if user wants do define their own conn and engine variables. False otherwise. Default = False.
  - driver: Driver used by sqlalchemy to connect with database. Only used if custom_connection = False. Default = 'ODBC Driver 17 for SQL Server'.
  - show_progress: True for printed output describing updates made to database. Default = False.
  - kwargs: Data types to be assigned to a column in format column_name: 'type'. If column not included, default = '[nvarchar] (255) Null'.
    - e.g.: Age: 'INT'.

#### populate_table
``` sh
populate_table(key:tuple, table_name:str, df, if_exists:str = 'append', schema:str = 'dbo', custom_connection:bool = False, driver:str = 'ODBC Driver 17 for SQL Server', show_progress:bool = False)
```
Adds rows to table in the database.
**Arguments:**
  - **key**: Tuple in format (server, database).
  - **table_name**: Name of table to be populated in database. 
  - **df**: Pandas DataFrame object that table will be populated with.
  - **if_exists**: Argument used to determine how table should be populated. Should be set to 'fail', 'replace', or 'append'. Default = 'append'.
  - schema: Schema in which table exists in database. Default = 'dbo'.
  - custom_connection: True if user wants do define their own conn and engine variables. False otherwise. Default = False.
  - driver: Driver used by sqlalchemy to connect with database. Only used if custom_connection = False. Default = 'ODBC Driver 17 for SQL Server'.
  - show_progress: True for printed output describing updates made to database. Default = False.

#### drop_table
```sh
drop_table(key:tuple, table_name:str, schema:str = 'dbo', custom_connection:bool = False, driver:str = 'ODBC Driver 17 for SQL Server', show_progress:bool = False)
```
Drops table from database. 
**Arguments:**
  -  **key**: Tuple in format ('server', 'database').
  -  **table_name**: Name of table to be dropped from database. 
  -  schema: Schema in which table exists in database. Default = 'dbo'.
  -  custom_connection: True if user wants do define their own conn and engine variables. False otherwise. Default = False.
  -  driver: Driver used by sqlalchemy to connect with database. Only used if custom_connection = False. Default = 'ODBC Driver 17 for SQL Server'.  
  -  show_progress: True for printed output describing updates made to database. Default = False.

#### trunc_table
```sh
trunc_table(key:tuple, table_name:str, schema:str = 'dbo', custom_connection:bool = False, driver:str = 'ODBC Driver 17 for SQL Server', show_progress:bool = False)
```
Truncates table in database.
**Arguments:**
  - **key**: Tuple in format (server, database).
  - **table_name**: Name of table to be truncated from database. 
  - schema: Schema in which table exists in database. Default = 'dbo'.
  - custom_connection: True if user wants do define their own conn and engine variables. False otherwise. Default = False.
  - driver: Driver used by sqlalchemy to connect with database. Only used if custom_connection = False. Default = 'ODBC Driver 17 for SQL Server'.
  - show_progress: True for printed output describing updates made to database. Default = False.  

#### delete_rows
```sh
delete_rows(key:tuple, table_name:str, schema:str = 'dbo', custom_connection:bool = False, driver:str = 'ODBC Driver 17 for SQL Server', show_progress:bool = False, **kwargs)
```
Removes specified rows from table in database. 
**Arguments:**
  - **key**: Tuple in format (server, database).
  - **table_name**: Name of table to be deleted from database. 
  - schema: Schema in which table exists in database. Default = 'dbo'.
  - custom_connection: True if user wants do define their own conn and engine variables. False otherwise. Default = False.
  - driver: Driver used by sqlalchemy to connect with database. Only used if custom_connection = False. Default = 'ODBC Driver 17 for SQL Server'.      
  - show_progress: True for printed output describing updates made to database. Default = False.
  - **kwargs**: Gives filters for rows to delete.
     - Eg. ```delete_rows(your_key, table_name='Employees', FirstName = 'Sam')``` deletes all rows of employees named Sam.
    ```delete_rows(your_key, table_name='Employees', FirstName = 'Sam', LastName = 'I Am')``` deletes Sam I Am from the employees table.

**Limitations:**: 
  - Column names must not have spaces/periods, etc.
  - Can only filter on =, not < or > ***(use sqlalchemy for this functionality).

#### custom_query
``` sh
custom_query(key:tuple, query:str,custom_connection:bool = False, driver:str = 'ODBC Driver 17 for SQL Server')
```
Runs custom query and returns resulting table.
**Arguments:**
  -  **key**: Tuple in format (server, database).
  -  **query**: String with custom SQL query.
  -  custom_connection: True if user wants do define their own conn and engine variables. False otherwise. Default = False. 
  -  driver: Driver used by sqlalchemy to connect with database. Only used if custom_connection = False. Default = 'ODBC  - Driver 17 for SQL Server'.
