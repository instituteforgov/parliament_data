# %%
# !/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Purpose
        Extract state of the parties between two dates
    Inputs
        - API: Parliament State of the Parties API
    Outputs
        - txt: logs/members_search_status_codes.txt
    Parameters
        - start_date (str): Start date
        - end_date (str): End date/datetime
    Notes
        None
'''

from datetime import datetime

import pandas as pd
import yaml

from functions import queryStateOfTheParties, extractStateOfTheParties

# %%
# READ IN CONFIG FILE
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# %%
# SET PARAMETERS
start_date = "2023-11-07"
end_date = datetime.today()

# %%
# CREATE LIST OF DATES
date_list = pd.date_range(start=start_date, end=end_date).to_pydatetime().tolist()

# %%
# QUERY API
state_of_the_parties = []

for date in date_list:

    state_of_the_parties_results = queryStateOfTheParties(
        date=date.date(),
        headers=config['headers'],
        house=1,
        save_logs=True
    )

    if state_of_the_parties_results:
        state_of_the_parties += extractStateOfTheParties(
            state_of_the_parties_results,
            date=date.date()
        )

# %%
# Turn list into df
df_state_of_the_parties = pd.DataFrame(state_of_the_parties, )

# %%
df_state_of_the_parties
