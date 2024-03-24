# %%
# !/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Purpose
        Identify changes a JSON file and a SQL table containing data
        on current members of parliament
    Inputs
        - JSON: members_url
            - URL to JSON file containing previous data
        - SQL: 'core.person'
    Outputs
        - SQL: workflow.task
            - See research database documentation
        - SQL: workflow.task_status
            - See research database documentation
        - SQL: workflow.task_allocation
            - See research database documentation
        - SQL: workflow.table
            - See research database documentation
        - SQL: workflow.{uuid}
            - See research database documentation
            - NB: In practice this is a UUID-based string, rather than a UUID
            as the format of a UUID is not a valid format for a SQL identifier
    Parameters
        - members_url: URL of JSON file containing new data
    Notes
        None
'''

import os
import uuid

import pandas as pd
from sqlalchemy import text

from ds_utils import database_operations as dbo
from ds_utils import dataframe_operations as dfo

# %%
# SET HOLD
raise SystemExit(0)

# %%
# SET PARAMETERS
members_url = 'https://raw.githubusercontent.com/instituteforgov/parliament_data/d8f69ff32c66f0f7e606d399e2f34261ce14d8f5/data/current_mps.json'      # noqa: E501

# %%
# CONNECT TO DATABASE
connection = dbo.connect_sql_db(
    driver='pyodbc',
    driver_version=os.environ['odbc_driver'],
    dialect='mssql',
    server=os.environ['odbc_server'],
    database=os.environ['odbc_database'],
    authentication=os.environ['odbc_authentication'],
    username=os.environ['odbc_username'],
)

# %%
# READ IN DATA
# JSON data
df_members = pd.read_json(members_url)

# D/b data
# FIXME: This currently uses a previous version of df_members in place of df_person_existing
# df_members_ hasn't been reshaped to match the structure we intend to use in the database yet

# df_person_existing = dbo.retry_sql_function(
#     function=pd.read_sql,
#     sql='''
#         select *
#         from testing.person_2
#     ''',
#     con=connection,
#     parse_dates=['start_date', 'end_date'],
#     dtype={
#         'id_parliament': 'Int64',
#     }
# )
person_existing_url = 'https://raw.githubusercontent.com/instituteforgov/parliament_data/838c042d8ec7b269f335051d7e3d3ebcaea20520/data/current_mps.json'      # noqa: E501
df_person_existing = pd.read_json(person_existing_url)

# %%
# IDENTIFY CHANGES
df_diff = dfo.identify_row_differences(
    df_members,
    df_person_existing,
    how='outer',
    drop_indicator_column=False,
    indicator_values=['prev', 'curr'],
    keep_rows='both',
    validate='1:1',
)

# %%
# SAVE CHANGES TO D/B
# Create new task
task_id = uuid.uuid4()

query = f'''
    insert into workflow.task values (
        \'{task_id}\',
        'sql_upsert',
        'Review identify_changes.py output'
    )
'''

with dbo.retry_sql_function(connection.begin) as conn:
    conn.execute(text(query))

# %%
# Create new task status
user_creator = os.environ['odbc_username']

query = f'''
    insert into workflow.task_status values (
        newid(),
        \'{task_id}\',
        'created',
        \'{user_creator}\',
        getdate()
    )
'''

with dbo.retry_sql_function(connection.begin) as conn:
    conn.execute(text(query))

# %%
# Create new task allocation
user_reviewer = os.environ['odbc_username']

query = f'''
    insert into workflow.task_allocation values (
        newid(),
        \'{task_id}\',
        'reviewer',
        \'{user_reviewer}\'
    )
'''

with dbo.retry_sql_function(connection.begin) as conn:
    conn.execute(text(query))

# %%
# Create new table

# Create table
# Converting dashes to underscores to make this a valid identifier and
# appending a prefix because identifiers can't start with a number
name = 't_' + str(uuid.uuid4()).replace('-', '_')

df_diff.to_sql(
    name,
    connection,
    schema='workflow',
    index=False,
)

# Save details of table created
query = f'''
    insert into workflow.[table] values (
        newid(),
        \'{task_id}\',
        'for_review',
        \'{name}\'
    )
'''

with dbo.retry_sql_function(connection.begin) as conn:
    conn.execute(text(query))

# %%
