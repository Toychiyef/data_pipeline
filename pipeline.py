# coding=utf-8

from sqlalchemy import create_engine

# Some database engine initialization
engine = create_engine('database_url')

# Other code ...

with engine.begin():
    # Correctly indented CREATE TABLE statement block
    create_table_sql = '''
    CREATE TABLE example (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );
    '''
    # Execute the SQL statement
    engine.execute(create_table_sql)

# Rest of the code ...
