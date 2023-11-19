# %%
# !/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Purpose
        Test extracting member details using GitHub Actions
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

import yaml

from functions import queryMembersSearchAPI


# %%
# READ IN CONFIG FILE
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# %%
# QUERY MEMBERS SEARCH API AND EXTRACT DATA TO DF
# Create empty lists that will hold member details and API status codes
members = []

# %%
# Pull data from API
i = 0
members_search_results = queryMembersSearchAPI(i, headers=config['headers'])

for i in range(0, math.ceil(members_search_results['totalResults'] / 20)):

    # Query Members Search API
    members += queryMembersSearchAPI(i * 20, headers=config['headers'])
