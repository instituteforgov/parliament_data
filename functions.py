# !/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

import log_operations as lo


def queryMembersSearchAPI(
    starting_number,
    headers,
    house=None,
    current_members=True,
    save_logs=True
):
    '''
    Requests details from the Members Search Parliament API

        Parameters:
            - starting_number (int): Starting result number
            - headers (str): Headers to use in the request
            - house (int): House to search (1 = Commons, 2 = Lords)
            - current_members (bool): Whether to return current members only
            - save_logs (bool): Whether to save logs to file

        Returns:
            - r (str): API results in JSON form if status code is 200,
            else None

        Notes:
            - 20 is the maximum number of results that can be pulled
            per query
    '''

    url = (
        'https://members-api.parliament.uk/api/Members/Search?' +
        'House=' + str(house) +
        '&IsCurrentMember=' + str(current_members) +
        '&skip=' + str(starting_number) + '&take=20'
    )

    r = requests.get(
        url,
        headers=headers
    )

    if save_logs:
        lo.log_details(
            logs_folder_path='logs',
            logs_file_name='members_search_status_codes.txt',
            message='Status code ' + str(r.status_code) + ' for url ' + url
        )

    if r.status_code == 200:
        r = r.json()
        return r
    else:
        return


def extractMembers(json):
    '''
    Extracts member details from json returned from the
    Members Search Parliament API

        Parameters:
            - json (str): API results in JSON form

        Returns:
            - members (list): List of dicts containing member details

        Notes:
            None
    '''

    members = []

    for member in json['items']:

        # Identify whether members are current
        # NB: membershipStatus is None for former members
        if member['value']['latestHouseMembership']['membershipStatus']:
            is_current = True
        else:
            is_current = False

        # Identify whether members are MPs
        # NB: latestHouseMembership is None for peers
        if member['value']['latestHouseMembership']['house'] == 1:
            is_mp = True
            is_peer = False
        else:
            is_mp = False
            is_peer = True

        dict = {}
        dict.update({
            'id': member['value']['id'],
            'gender': member['value']['gender'],
            'nameDisplayAs': member['value']['nameDisplayAs'],
            'is_mp': is_mp,
            'is_peer': is_peer,
            'is_current': is_current,
            'party': member['value']['latestParty']['name'],
            'constituency': member['value']['latestHouseMembership']['membershipFrom'],
        })
        members.append(dict)

    return members
