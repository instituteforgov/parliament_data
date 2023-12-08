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
        None
    Parameters
        - See config.yaml
    Notes
        - See technical_documentation.md for details of issues of consistency of
        record structure that are addressed here
'''

import math
import uuid

import pandas as pd
import yaml

from functions import (
    queryMembersSearchAPI, extractMembers, queryMembersHistoryAPI, extractMembersHistory
)
from ds_utils import string_operations as so

# %%
# READ IN CONFIG FILE
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# %%
# QUERY MEMBERS SEARCH API AND EXTRACT DATA TO DF
# Make initial API query, to get total number of results
members_search_results = queryMembersSearchAPI(
    starting_number=0, headers=config['headers']
)

# Pull data from API
members_search_results = [
    queryMembersSearchAPI(starting_number=i * 20, headers=config['headers'])
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
# READ IN TEMPORARY COPY OF DATA
# XXX
df_members = pd.read_pickle('temp/members.pkl')
df_name_histories = pd.read_pickle('temp/name_histories.pkl')
df_party_histories = pd.read_pickle('temp/party_histories.pkl')
df_house_membership_histories = pd.read_pickle('temp/house_membership_histories.pkl')

# %%
# CARRY OUT GENERAL CLEANING
# Clean name
df_members['nameClean'] = df_members['nameDisplayAs'].apply(
    lambda x: so.strip_name_title(x, exclude_peerage=True)
)

df_name_histories['nameClean'] = df_name_histories['nameDisplayAs'].apply(
    lambda x: so.strip_name_title(x, exclude_peerage=True)
)

# %%
# Convert date strings to datetimes
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

# %%
# FIX CONSISTENCY OF RECORD STRUCTURE
# 1. Set party history and membership history end dates to election date rather
# than start of pre-election period for elections since 2015
# NB: This makes May 2015-onwards data consistent with data before this point
preelection_period_to_election_date = config['mappings']['preelection_period_to_election_date']

df_party_histories['endDate'] = df_party_histories['endDate'].map(
    lambda x: preelection_period_to_election_date.get(x, x)
)
df_house_membership_histories['endDate'] = df_house_membership_histories['endDate'].map(
    lambda x: preelection_period_to_election_date.get(x, x)
)

# %%
# 2. Break MPs' pre-2015 party history records into multiple records where they span
# multiple parliaments
# NB: This makes pre-2015 data consistent with data from 2015 onwards

# Identify first house membership history record where someone is in the Lords
df_lords_membership_start_dates = df_house_membership_histories.loc[
    df_house_membership_histories['house'] == 2
].groupby('id').agg({
    'startDate': 'min'
}).reset_index()

# Add house to party history records
df_party_histories['house'] = df_party_histories.merge(
    df_lords_membership_start_dates, on='id', how='left'
).apply(
    lambda x: 2 if x['startDate_x'] >= x['startDate_y'] else 1,
    axis=1
)

# Create dataframe of pre-2015, MP party history records
df_party_histories_mps_pre2015 = df_party_histories.loc[
    (df_party_histories['endDate'] <= pd.to_datetime('2015-05-07T00:00:00')) &
    (df_party_histories['house'] == 1)
].copy()

# Create date_range for each record
# NB: Stopping before endDate so that in the subsequent steps we don't add a record
# for the period beginning when the MP left the Commons
df_party_histories_mps_pre2015.loc[
    :, 'date_range'
] = df_party_histories_mps_pre2015.apply(
    lambda x:
        pd.date_range(
            start=x['startDate'],
            end=x['endDate'] - pd.Timedelta(1, unit='D'),
            freq='D'
        ).tolist(),
    axis=1
)

# Delete items in date_range that are not either the first item or an election date
pre2015_election_dates = config['constants']['pre2015_election_dates']

df_party_histories_mps_pre2015.loc[
    :, 'date_range'
] = df_party_histories_mps_pre2015.apply(
    lambda x:
        [
            y for y in x['date_range']
            if y == x['date_range'][0] or y in pre2015_election_dates
        ],
    axis=1
)

# Explode date_range and rename as startDate
df_party_histories_mps_pre2015 = df_party_histories_mps_pre2015.explode('date_range')
df_party_histories_mps_pre2015['startDate'] = df_party_histories_mps_pre2015['date_range']
df_party_histories_mps_pre2015.drop(columns=['date_range'], inplace=True)

# Set endDate as the first date from pre2015_election_dates after startDate, or
# 2015-05-07T00:00:00 if there is no such date
df_party_histories_mps_pre2015.loc[
    :, 'endDate'
] = df_party_histories_mps_pre2015.apply(
    lambda x:
        [
            y for y in pre2015_election_dates + [pd.to_datetime('2015-05-07T00:00:00')]
            if y > x['startDate']
        ][0],
    axis=1
)

# Append pre-2015 MP party history records to other records
df_party_histories = pd.concat(
    [
        df_party_histories_mps_pre2015,
        df_party_histories.loc[
            (df_party_histories['endDate'] > pd.to_datetime('2015-05-07T00:00:00')) |
            (df_party_histories['house'] == 2)
        ]
    ]
)

# %%
# 3. Collapse name history records taking earliest startDate and latest endDate
# for a given id and nameClean
# NB: These arise where a. another form of someone's name (e.g. nameAddressAs) has changed
# in the parliament data even where nameDisplayAs hasn't, b. where our cleaning of names
# has created additional redundant records
# NB: mask() replaces values with NaT where one value is NaT in that column for that ID - needed
# as max() otherwise favours known dates over missing dates, where missing dates indicate
# something is ongoing
# Ref: https://stackoverflow.com/a/71299818/4659442
df_name_histories = df_name_histories.groupby(['id', 'nameClean']).agg({
    'startDate': 'min',
    'endDate': 'max'
}).mask(
    df_name_histories[['startDate', 'endDate']].isna().groupby([
        df_name_histories['id'], df_name_histories['nameClean']
    ]).max()
).reset_index()

# %%
# BUILD FINAL DATAFRAMES
# Build person table
# TODO: Currently no attempt to create short_name column

# Create base table
df_person = df_name_histories.merge(
    df_members[['id', 'gender']],
    on='id',
    how='left',
).rename(
    columns={
        'id': 'parliament_id',
        'nameClean': 'name',
        'startDate': 'start_date',
        'endDate': 'end_date',
    }
)[['parliament_id', 'name', 'gender', 'start_date', 'end_date']]

# Add UUID
# Ref: https://stackoverflow.com/a/48975426/4659442
df_person.insert(
    0, 'id',
    df_person.groupby('parliament_id')['name'].transform(lambda x: uuid.uuid4())
)

