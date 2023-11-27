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
        None
    Notes
        None
'''

import math

import pandas as pd
import yaml

from functions import (
    queryMembersSearchAPI, extractMembers, queryMembersHistoryAPI, extractMembersHistory
)
from utils import string_operations as so


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

