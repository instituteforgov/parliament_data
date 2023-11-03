# %%
# !/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Purpose
        Extract member details
    Inputs
        API: Parliament Members Search API
    Outputs
        - txt: logs/members_search_status_codes.txt
    Parameters
        None
    Notes
        None
'''

import math

import pandas as pd
import yaml

from functions import queryMembersSearchAPI, extractMembers
import string_operations as so


# %%
# READ IN CONFIG FILE
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# %%
# QUERY MEMBERS SEARCH API AND EXTRACT DATA TO DF
# Create empty lists that will hold member details and API status codes
members = []

# %%
# Make initial API query, to get total number of results
i = 0
members_search_results = queryMembersSearchAPI(i, headers=config['headers'])

for i in range(0, math.ceil(members_search_results['totalResults'] / 20)):

    # Query Members Search API
    members_search_results = queryMembersSearchAPI(i * 20, headers=config['headers'])

    # Extract member details from a page of results
    if members_search_results:
        members += extractMembers(members_search_results)

# Turn list into dfs
# df_membersstatuscodes = pd.DataFrame(membersstatuscodes, )
df_members = pd.DataFrame(members, )

# %%
df_members.loc[
    df_members['is_mp']
]

# %%
# CLEAN DATA
# Clean name
df_members['nameClean'] = [
    so.strip_name_title(i, exclude_peerage=True) for i in df_members['nameDisplayAs']
]

# %%
df_members
