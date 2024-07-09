# %%
# !/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Purpose
        Extract member details
    Inputs
        - API: Parliament Members Search API
        - API: Parliament Members History API
    Outputs
        - pkl: data/<date>/members.pkl
        - pkl: data/<date>/name_histories.pkl
        - pkl: data/<date>/party_histories.pkl
        - pkl: data/<date>/house_membership_histories.pkl
        - SQL: core.person
        - SQL: core.representation
        - SQL: core.representation_characteristics
        - SQL: core.constituency
    Parameters
        - General: Stored in config.yaml
        - run_date: Run date
        - Database connection: Stored in environment variables
    Notes
        - See technical_documentation.md for details of issues of consistency of
        record structure that are addressed here
'''

import os
import math
import uuid

from nameparser import HumanName
import pandas as pd
from sqlalchemy.dialects.mssql import DATE, NVARCHAR, SMALLINT, UNIQUEIDENTIFIER
import yaml

from functions import (
    queryMembersSearchAPI, extractMembers, queryMembersHistoryAPI, extractMembersHistory
)
from ds_utils import database_operations as dbo
from ds_utils import string_operations as so

# %%
# READ IN CONFIG FILE
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# %%
# SET PARAMETERS
run_date = '20240709'

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
# QUERY MEMBERS SEARCH API AND EXTRACT DATA TO DF
# Make initial API query, to get total number of results
members_search_results = queryMembersSearchAPI(
    starting_number=0, headers=config['headers'], current_members=None
)

# # Pull data from API
members_search_results = [
    queryMembersSearchAPI(starting_number=i * 20, headers=config['headers'], current_members=None)
    for i in range(0, math.ceil(members_search_results['totalResults'] / 20))
]

# Extract member details
members = []

for record in members_search_results:
    if record:
        members += extractMembers(record)

df_members = pd.DataFrame(members, )

# %%
# QUERY MEMBERS HISTORY API AND EXTRACT DATA TO DFS
# Pull data from API
members_history_results = [
    queryMembersHistoryAPI(x, headers=config['headers'])
    for x in df_members['id']
]

# Extract member history details
# NB: Done in two steps as tuples can't be used for augmented assignment
name_histories = []
party_histories = []
house_membership_histories = []

for record in members_history_results:
    if record:
        (
            name_histories_record,
            party_histories_record,
            house_membership_histories_record
        ) = extractMembersHistory(
            record
        )
        name_histories += name_histories_record
        party_histories += party_histories_record
        house_membership_histories += house_membership_histories_record

df_name_histories = pd.DataFrame(name_histories)
df_party_histories = pd.DataFrame(party_histories)
df_house_membership_histories = pd.DataFrame(house_membership_histories)

# %%
# SAVE COPY OF DATA
# Create folder if not exists
if not os.path.exists('data/' + run_date):
    os.makedirs('data/' + run_date)

# Save data
df_members.to_pickle('data/' + run_date + '/members.pkl')
df_name_histories.to_pickle('data/' + run_date + '/name_histories.pkl')
df_party_histories.to_pickle('data/' + run_date + '/party_histories.pkl')
df_house_membership_histories.to_pickle('data/' + run_date + '/house_membership_histories.pkl')

# %%
# CLEAN AND AUGMENT DATA
# Strip titles from names
df_members['name'] = df_members['nameDisplayAs'].apply(
    lambda x: so.strip_name_title(x, exclude_peerage=True)
)

df_name_histories['name'] = df_name_histories['nameDisplayAs'].apply(
    lambda x: so.strip_name_title(x, exclude_peerage=True)
)

# Remove commas and full stops from names
# NB: This fixes occasional issues such as 'Angela, E. Smith'
df_members.loc[:, 'name'] = df_members['name'].str.replace(',', '').str.replace('.', '')
df_name_histories.loc[:, 'name'] = (
    df_name_histories['name'].str.replace(',', '').str.replace('.', '')
)

# Drop nameDisplayAs columns
df_members.drop(columns='nameDisplayAs', inplace=True)
df_name_histories.drop(columns='nameDisplayAs', inplace=True)

# Add short_name columns
df_members['short_name'] = df_members['name'].apply(
    lambda x:
        so.split_title_names(x)[1] if ' of ' in x and so.split_title_names(x)[1] else
        so.split_title_names(x)[2] if ' of ' in x else
        HumanName(x).last or pd.NA
)
df_name_histories['short_name'] = df_name_histories['name'].apply(
    lambda x:
        so.split_title_names(x)[1] if ' of ' in x and so.split_title_names(x)[1] else
        so.split_title_names(x)[2] if ' of ' in x else
        HumanName(x).last or pd.NA
)

# Convert date strings to datetimes
df_members['statusStartDate'] = pd.to_datetime(df_members['statusStartDate'])
df_name_histories['startDate'] = pd.to_datetime(df_name_histories['startDate'])
df_name_histories['endDate'] = pd.to_datetime(df_name_histories['endDate'])
df_party_histories['startDate'] = pd.to_datetime(df_party_histories['startDate'])
df_party_histories['endDate'] = pd.to_datetime(df_party_histories['endDate'])
df_house_membership_histories['startDate'] = pd.to_datetime(
    df_house_membership_histories['startDate']
)
df_house_membership_histories['endDate'] = pd.to_datetime(
    df_house_membership_histories['endDate']
)

# Drop statusStartDate column
df_members.drop(columns='statusStartDate', inplace=True)

# Rename columns
df_members.rename(columns={'id': 'id_parliament'}, inplace=True)
df_members.rename(columns={'membershipFromID': 'constituency_id'}, inplace=True)
df_members.rename(columns={'membershipFrom': 'constituency'}, inplace=True)

df_name_histories.rename(columns={'id': 'id_parliament'}, inplace=True)
df_name_histories.rename(columns={'startDate': 'start_date'}, inplace=True)
df_name_histories.rename(columns={'endDate': 'end_date'}, inplace=True)

df_party_histories.rename(columns={'id': 'id_parliament'}, inplace=True)
df_party_histories.rename(columns={'startDate': 'start_date'}, inplace=True)
df_party_histories.rename(columns={'endDate': 'end_date'}, inplace=True)

df_house_membership_histories.rename(columns={'id': 'id_parliament'}, inplace=True)
df_house_membership_histories.rename(
    columns={'membershipFromID': 'constituency_id_parliament'}, inplace=True
)
df_house_membership_histories.rename(columns={'membershipFrom': 'constituency_name'}, inplace=True)
df_house_membership_histories.rename(columns={'startDate': 'start_date'}, inplace=True)
df_house_membership_histories.rename(columns={'endDate': 'end_date'}, inplace=True)

# Augment house membership histories data

# 1. Add UUIDs
df_members['id'] = [uuid.uuid4() for _ in range(
    len(df_members)
)]
df_name_histories['id'] = [uuid.uuid4() for _ in range(
    len(df_name_histories)
)]
df_party_histories['id'] = [uuid.uuid4() for _ in range(
    len(df_party_histories)
)]
df_house_membership_histories['id'] = [uuid.uuid4() for _ in range(
    len(df_house_membership_histories)
)]

# 2. Code house to Commons, Lords
df_house_membership_histories['house'] = df_house_membership_histories['house'].map(
    lambda x: 'Commons' if x == 1 else 'Lords'
)

# 3. Create peerage type column
# NB: The x['house'] == 'Lords' is required as some older Commons records
# erroneously have constituency_id_parliament <= 10
df_house_membership_histories.insert(
    3,
    'type',
    df_house_membership_histories.apply(
        lambda x:
        config['peerage_type_renamings'][x['constituency_name']]
            if x['constituency_name'] in config['peerage_type_renamings'].keys()
        else x['constituency_name'].capitalize()
            if x['house'] == 'Lords' and x['constituency_id_parliament'] <= 10
        else pd.NA,
        axis=1
    )
)

# 4. Set constituency_id_parliament, constituency_name to NA for Lords records
df_house_membership_histories.loc[
    df_house_membership_histories['house'] == 'Lords', 'constituency_id_parliament'
] = pd.NA
df_house_membership_histories.loc[
    df_house_membership_histories['house'] == 'Lords', 'constituency_name'
] = pd.NA

# 5. Create constituency_id column
# Ref: https://stackoverflow.com/a/48975426/4659442
df_house_membership_histories.loc[
    df_house_membership_histories['house'] == 'Commons',
    'constituency_id'
] = df_house_membership_histories.loc[
    df_house_membership_histories['house'] == 'Commons'
].groupby('constituency_id_parliament')['house'].transform(lambda x: uuid.uuid4())

# Reorder columns
df_members = df_members[[
    'id', 'id_parliament', 'name', 'short_name',
    'gender', 'is_mp', 'is_peer', 'is_current',
    'party', 'constituency_id', 'constituency'
]]

df_name_histories = df_name_histories[[
    'id', 'id_parliament', 'name', 'short_name', 'start_date', 'end_date'
]]

df_party_histories = df_party_histories[[
    'id', 'id_parliament', 'party', 'start_date', 'end_date'
]]

df_house_membership_histories = df_house_membership_histories[[
    'id', 'id_parliament', 'house', 'type', 'constituency_id',
    'constituency_id_parliament', 'constituency_name', 'start_date', 'end_date'
]]

# %%
# FIX CONSISTENCY OF RECORD STRUCTURE
# 1. Set party history and membership history end dates to election date rather
# than start of pre-election period for elections since 2015
# NB: This makes May 2015-onwards data consistent with data before this point
preelection_period_to_election_date = config['mappings']['preelection_period_to_election_date']

df_party_histories['end_date'] = df_party_histories['end_date'].map(
    lambda x: preelection_period_to_election_date.get(x, x)
)
df_house_membership_histories['end_date'] = df_house_membership_histories['end_date'].map(
    lambda x: preelection_period_to_election_date.get(x, x)
)

# %%
# 2. Collapse name history records taking earliest start_date and latest end_date
# for a given id and name
# NB: These arise where a. another form of someone's name (e.g. nameAddressAs) has changed
# in the parliament data even where nameDisplayAs hasn't, b. where our cleaning of names
# has created additional redundant records
# NB: mask() replaces values with NaT where one value is NaT in that column for that ID - needed
# as max() otherwise favours known dates over missing dates, where missing dates indicate
# something is ongoing. Ref: https://stackoverflow.com/a/71299818/4659442
# NB: In some cases parliament data erroneously contains near-duplicate name records (e.g.
# Mary Kelly Foy, 4753) - these aren't fixed here and need to be fixed downstream in SQL
df_name_histories = df_name_histories.groupby(['id_parliament', 'name', 'short_name']).agg({
    'start_date': 'min',
    'end_date': 'max'
}).mask(
    df_name_histories[['start_date', 'end_date']].isna().groupby([
        df_name_histories['id_parliament'], df_name_histories['name']
    ]).max()
).reset_index()

# %%
# SAVE DATA TO D/B
df_members.to_sql(
    name='parliament_members',
    con=connection,
    schema='testing',
    if_exists='replace',
    index=False,
    dtype={
        'id': UNIQUEIDENTIFIER,
        'id_parliament': SMALLINT,
        'name': NVARCHAR(256),
        'short_name': NVARCHAR(256),
        'gender': NVARCHAR(1),
        'start_date': DATE,
        'end_date': DATE,
    },
)

df_name_histories.to_sql(
    name='parliament_name_histories',
    con=connection,
    schema='testing',
    if_exists='replace',
    index=False,
    dtype={
        'id': UNIQUEIDENTIFIER,
        'id_parliament': SMALLINT,
        'name': NVARCHAR(256),
        'short_name': NVARCHAR(256),
        'start_date': DATE,
        'end_date': DATE,
    },
)

df_party_histories.to_sql(
    name='parliament_party_histories',
    con=connection,
    schema='testing',
    if_exists='replace',
    index=False,
    dtype={
        'id': UNIQUEIDENTIFIER,
        'id_parliament': SMALLINT,
        'party': NVARCHAR(256),
        'start_date': DATE,
        'end_date': DATE,
    },
)

df_house_membership_histories.to_sql(
    name='parliament_house_membership_histories',
    con=connection,
    schema='testing',
    if_exists='replace',
    index=False,
    dtype={
        'id': UNIQUEIDENTIFIER,
        'id_parliament': SMALLINT,
        'house': NVARCHAR(32),
        'type': NVARCHAR(32),
        'constituency_id': UNIQUEIDENTIFIER,
        'constituency_id_parliament': SMALLINT,
        'constituency_name': NVARCHAR(256),
        'start_date': DATE,
        'end_date': DATE
    },
)

# %%
