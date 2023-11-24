# %%
# !/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Purpose
        Test extracting member details using GitHub Actions
    Inputs
        API: Parliament Members Search API
    Outputs
        - json: data/current_mps.json
    Parameters
        None
    Notes
        None
'''

import json
import math

import yaml

from functions import queryMembersSearchAPI, extractMembers


# %%
# READ IN CONFIG FILE
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# %%
# QUERY MEMBERS SEARCH API AND EXTRACT DATA TO DF
# Create empty lists that will hold member details
members = []

# %%
# Pull data from API
i = 0
members_search_results = queryMembersSearchAPI(
    i,
    headers=config['headers'],
    house=1,
    current_members=True,
    save_logs=False
)

for i in range(0, math.ceil(members_search_results['totalResults'] / 20)):

    # Query Members Search API
    members_search_results = queryMembersSearchAPI(
        i * 20,
        headers=config['headers'],
        house=1,
        current_members=True,
        save_logs=False
    )

    # Extract member details from a page of results
    if members_search_results:
        members += extractMembers(members_search_results)

# %%
# Save output
with open('data/current_mps.json', 'w') as f:
    json.dump(members, f)
