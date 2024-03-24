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
        TODO
    Parameters
        XXX
    Notes
        None
'''

# import os

import pandas as pd

# from ds_utils import database_operations as dbo
from ds_utils import dataframe_operations as dfo

# %%
# SET PARAMETERS
members_url = 'https://raw.githubusercontent.com/instituteforgov/parliament_data/d8f69ff32c66f0f7e606d399e2f34261ce14d8f5/data/current_mps.json'      # noqa: E501

# %%
# READ IN DATA
# JSON data
df_members = pd.read_json(members_url)

# D/b data
# FIXME: This currently uses a previous version of df_members in place of df_person_existing
# df_members_ hasn't been reshaped to match the structure we intend to use in the database yet
# connection = dbo.connect_sql_db(
#     driver='pyodbc',
#     driver_version=os.environ['odbc_driver'],
#     dialect='mssql',
#     server=os.environ['odbc_server'],
#     database=os.environ['odbc_database'],
#     authentication=os.environ['odbc_authentication'],
#     username=os.environ['odbc_username'],
# )

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
# Identify changes between JSON file and SQL table
dfo.identify_row_differences(
    df_members,
    df_person_existing,
    how='outer',
    drop_indicator_column=False,
    indicator_values=['prev', 'curr'],
    keep_rows='both',
    validate='1:1',
)

# %%
