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
            (pd.isna(df_party_histories['endDate'])) |
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

# Set start_date/end_date to NaT where there isn't a corresponding end_date/start_date
# NB: In many cases these will be dates of birth and dates of death, which we aren't interested
# in. In the case of Lords, a start_date will in many cases be a date of ennoblement, which would
# correspond to a name change - but in the case of newly elected hereditary peers it may not be
df_person['start_date_matches_end_date'] = df_person.apply(
    lambda x:
        not df_person.loc[
            (x['parliament_id'] == df_person['parliament_id']) &
            (x['start_date'] == df_person['end_date'])
        ].empty,
    axis=1
)

df_person['end_date_matches_start_date'] = df_person.apply(
    lambda x:
        not df_person.loc[
            (x['parliament_id'] == df_person['parliament_id']) &
            (x['end_date'] == df_person['start_date'])
        ].empty,
    axis=1
)

df_person.loc[
    ~df_person['start_date_matches_end_date'],
    'start_date'
] = pd.NA

df_person.loc[
    ~df_person['end_date_matches_start_date'],
    'end_date'
] = pd.NA

df_person.drop(
    columns=['start_date_matches_end_date', 'end_date_matches_start_date'],
    inplace=True
)

# Add UUID
# Ref: https://stackoverflow.com/a/48975426/4659442
df_person.insert(
    0, 'id',
    df_person.groupby('parliament_id')['name'].transform(lambda x: uuid.uuid4())
)

# %%
# Build representation, constituency tables

# Create base table
df_representation = df_house_membership_histories[[
    'id', 'house', 'membershipFrom', 'membershipFromID', 'startDate', 'endDate'
]].rename(
    columns={
        'id': 'parliament_id',
        'startDate': 'start_date',
        'endDate': 'end_date',
    }
)

# Add UUID
# NB: Here we want it to be unique for each row
df_representation.insert(0, 'id', [uuid.uuid4() for _ in range(len(df_representation))])

# Add person_id
df_representation = df_representation.merge(
    df_person[['id', 'parliament_id']].drop_duplicates(),
    on='parliament_id',
    how='inner',
    suffixes=(None, '_y'),
    validate='many_to_one'
).rename(
    columns={
        'id_y': 'person_id'
    }
)

# Code house to Commons, Lords
df_representation['house'] = df_representation['house'].map(
    lambda x: 'Commons' if x == 1 else 'Lords'
)

# Save membershipFrom to type column where it relates to peerage type
# NB: See technical_documentation.md for details
df_representation.insert(
    3, 'type',
    df_representation.apply(
        lambda x: x['membershipFrom'].capitalize() if x['membershipFromID'] <= 10 else pd.NA,
        axis=1
    )
)

# Convert membershipFromID to constituency_id
# Ref: https://stackoverflow.com/a/48975426/4659442
df_representation.insert(
    4, 'constituency_id',
    df_representation.groupby('membershipFromID')['house'].transform(lambda x: uuid.uuid4())
)

# Build constituency table, excluding rows relating to peerages
df_constituency = df_representation.loc[
    df_representation['type'].isna()
][[
    'constituency_id', 'membershipFrom', 'membershipFromID'
]].drop_duplicates().rename(
    columns={
        'constituency_id': 'id',
        'membershipFrom': 'name',
        'membershipFromID': 'parliament_id'
    }
)

# Drop membershipFrom, membershipFromID columns
df_representation.drop(
    columns=['membershipFrom', 'membershipFromID'],
    inplace=True
)

# Drop rows where start_date is NaT
# NB: These appear to be erroneous/near-duplicate records for hereditary peers

# Check that these are all near-duplicates of something that does feature a start_date
df_representation_has_start_date = df_representation.loc[
    df_representation['start_date'].notna()
]

assert df_representation.loc[
    (df_representation['start_date'].isna()) &
    (
        ~df_representation['person_id'].isin(
            df_representation_has_start_date['person_id']
        )
    )
].empty, 'Not everyone in df_representation without a start_date has a record that \
    includes a start_date'

# Drop rows
df_representation.drop(
    df_representation.loc[
        df_representation['start_date'].isna()
    ].index,
    inplace=True
)

# Reorder columns
df_representation = df_representation[[
    'id', 'person_id', 'parliament_id', 'house', 'type', 'start_date', 'end_date'
]]

# %%
# Build representation history table

# Create base table, including all possible combinations of representation and
# party history
df_representation_characteristics = df_representation.merge(
    df_party_histories,
    left_on='parliament_id',
    right_on='id',
    how='left',
    suffixes=('_r', '_ph'),
).rename(
    columns={
        'id_r': 'representation_id',

    }
)

# Restrict to records where representation characteristics period falls
# within representation period
# NB: Here startDate is start date from df_party_histories
df_representation_characteristics = df_representation_characteristics[
    (
        df_representation_characteristics['start_date'] <=
        df_representation_characteristics['startDate']
    ) &
    (
        (pd.isna(df_representation_characteristics['end_date'])) |
        (
            df_representation_characteristics['end_date'] >
            df_representation_characteristics['startDate']
        )
    )
]

# Drop and rename columns
df_representation_characteristics = df_representation_characteristics[[
    'representation_id', 'party', 'startDate', 'endDate'
]].rename(
    columns={
        'startDate': 'start_date',
        'endDate': 'end_date',
    }
)

# Add UUID
# NB: Here we want it to be unique for each row
df_representation_characteristics.insert(
    0, 'id',
    [uuid.uuid4() for _ in range(len(df_representation_characteristics))]
)

# %%
# Build representation status table

# Create base table
df_representation_status = df_members[['id', 'status', 'statusNotes', 'statusStartDate']].rename(
    columns={
        'id': 'parliament_id',
        'status': 'status',
        'statusNotes': 'reason',
        'statusStartDate': 'start_date',
    }
)

# Recode status and make sentence case
df_representation_status['status'] = df_representation_status['status'].apply(
    lambda x: x.replace('Current Member', 'Active').capitalize() if pd.notna(x) else x
)

# Make notes sentence case
df_representation_status['reason'] = df_representation_status.apply(
    lambda x:
        None if x['reason'] == x['status'] else
        x['reason'].capitalize() if pd.notna(x['reason']) else
        x['reason'],
    axis=1
)

# Add NaT end date
df_representation_status['end_date'] = pd.NA

# Add representation_id
df_representation_status = df_representation_status.merge(
    df_representation,
    on='parliament_id',
    how='left',
    suffixes=('_rs', '_r')
).rename(
    columns={
        'id': 'representation_id',
        'start_date_rs': 'start_date',
        'end_date_rs': 'end_date',
    }
)

df_representation_status = df_representation_status[
    (df_representation_status['start_date'] >= df_representation_status['start_date_r']) &
    (
        (pd.isna(df_representation_status['end_date_r'])) |
        (df_representation_status['end_date'] <= df_representation_status['end_date_r'])
    )
]

# Check that only one record per person
# NB: This should be true, as the API only returns a member's latest status
assert df_representation_status.groupby('parliament_id').size().max() == 1, 'More than one \
    representation status record for at least one person'

# Drop columns
df_representation_status = df_representation_status[[
    'representation_id', 'status', 'reason', 'start_date', 'end_date'
]]

# Add UUID
# NB: Here we want it to be unique for each row
df_representation_status.insert(
    0, 'id',
    [uuid.uuid4() for _ in range(len(df_representation_status))]
)
