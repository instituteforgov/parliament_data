# !/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Literal, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ds_utils import log_operations as lo


def queryMembersSearchAPI(
    starting_number: int,
    headers: dict,
    connection_retries: int = 5,
    backoff_factor: float = 0.5,
    house: Literal[1, 2, None] = None,
    current_members: Literal[True, False, None] = True,
    save_logs: bool = True,
) -> Optional[dict]:
    '''
    Requests details from the Members Search Parliament API

        Parameters:
            - starting_number (int): Starting result number
            - headers (str): Headers to use in the request
            - connection_retries (int): Number of retries for connection errors
            - backoff_factor (float): Backoff factor for connection errors
            - house (int): House to search (1 = Commons, 2 = Lords, None = both)
            - current_members (bool): Whether to return current members, former
            members or both (True = current, False = former, None = both)
            - save_logs (bool): Whether to save logs to file

        Returns:
            - r (str): API results in JSON form if status code is 200,
            else None

        Notes:
            - House is an optional argument for the Members Search API,
            unlike for the State of the Parties API
            - Passing any values other than 1 or 2 as the House parameter,
            and any values other than True or False as the IsCurrentMember parameter,
            is equivalent to not passing a value for that parameter
            - 20 is the maximum number of results that can be pulled
            per query
    '''
    if house not in [1, 2, None]:
        raise ValueError('House must be 1 (Commons), 2 (Lords) or None (both)')
    if current_members not in [True, False, None]:
        raise ValueError('current_members must be True (current), False (former) or None (both)')

    url = (
        'https://members-api.parliament.uk/api/Members/Search?' +
        'House=' + str(house) +
        '&IsCurrentMember=' + str(current_members) +
        '&skip=' + str(starting_number) + '&take=20'
    )

    session = requests.Session()
    retry = Retry(connect=connection_retries, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)

    r = session.get(url, headers=headers)

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


def extractMembers(json: dict) -> list:
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
            'party': (
                member['value']['latestParty']['name'] if
                member['value']['latestParty'] else
                None
            ),
            'constituency': (member['value']['latestHouseMembership']['membershipFrom']),
            'status': (
                member['value']['latestHouseMembership']['membershipStatus']['statusDescription'] if
                member['value']['latestHouseMembership']['membershipStatus'] else
                None
            ),
            'statusNotes': (
                member['value']['latestHouseMembership']['membershipStatus']['statusNotes'] if
                member['value']['latestHouseMembership']['membershipStatus'] else
                None
            ),
            'statusStartDate': (
                member['value']['latestHouseMembership']['membershipStatus']['statusStartDate'] if
                member['value']['latestHouseMembership']['membershipStatus'] else
                None
            ),
        })
        members.append(dict)

    return members


def queryMembersHistoryAPI(
    member_id: int,
    headers: dict,
    connection_retries: int = 5,
    backoff_factor: float = 0.5,
    save_logs: bool = True
) -> Optional[dict]:
    '''
    Requests details from the Members History Parliament API

        Parameters:
            - member_id (int): Member id number
            - headers (str): Headers to use in the request
            - connection_retries (int): Number of retries for connection errors
            - backoff_factor (float): Backoff factor for connection errors
            - save_logs (bool): Whether to save logs to file

        Returns:
            - r (str): API results in JSON form if status code is 200,
            else None

        Notes:
            None
    '''
    if not isinstance(member_id, int):
        raise TypeError('Member ID must be an integer')

    url = (
        'https://members-api.parliament.uk/api/Members/History?ids=' +
        str(member_id)
    )

    session = requests.Session()
    retry = Retry(connect=connection_retries, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)

    r = session.get(url, headers=headers)

    if save_logs:
        lo.log_details(
            logs_folder_path='logs',
            logs_file_name='members_search_history_status_codes.txt',
            message='Status code ' + str(r.status_code) + ' for url ' + url
        )

    if r.status_code == 200:
        r = r.json()
        return r
    else:
        return


def extractMembersHistory(json: dict) -> list:
    '''
    Extracts member details from json returned from the
    Members History Parliament API, adding details to
    list_namehistory, list_partyhistory, list_housemembershiphistory

        Parameters:
            - json (str): API results in JSON form

        Returns:
            - name_histories (list): List of dicts containing name history details
            - party_histories (list): List of dicts containing party history details
            - house_membership_histories (list): List of dicts containing house membership
            history details

        Notes:
            None
    '''

    name_histories = []
    party_histories = []
    house_membership_histories = []

    for member in json:
        id = member['value']['id']

        # Extract details from nameHistory list
        # NB: 'nameDisplayAs' is the best option, though for older records
        # it does include titles
        for record in member['value']['nameHistory']:
            dict = {}
            dict.update({
                'id': id,
                'startDate': record['startDate'],
                'endDate': record['endDate'],
                'nameDisplayAs': record['nameDisplayAs'],
            })
            name_histories.append(dict)

        # Extract details from partyHistory list
        for record in member['value']['partyHistory']:
            dict = {}
            dict.update({
                'id': id,
                'startDate': record['startDate'],
                'endDate': record['endDate'],
                'party': record['party']['name'],
            })
            party_histories.append(dict)

        # Extract details from houseMembershipHistory list
        for record in member['value']['houseMembershipHistory']:
            dict = {}
            dict.update({
                'id': id,
                'startDate': record['membershipStartDate'],
                'endDate': record['membershipEndDate'],
                'membershipFrom': record['membershipFrom'],
                'membershipFromID': record['membershipFromId'],
                'house': record['house'],
            })
            house_membership_histories.append(dict)

    return name_histories, party_histories, house_membership_histories


def queryConstituencySearchAPI(
    starting_number: int,
    headers: dict,
    save_logs: bool = True
) -> Optional[dict]:
    '''
    Requests details from the Constituency Search Parliament API

        Parameters:
            - starting_number (int): Starting result number
            - headers (str): Headers to use in the request
            - save_logs (bool): Whether to save logs to file

        Returns:
            - r (str): API results in JSON form if status code is 200,
            else None

        Notes:
            - 20 is the maximum number of results that can be pulled
            per query
            - This only returns current constituencies. Parliament APIs
            will return details on historic constituencies, but these
            aren't available via this endpoint
    '''
    if not isinstance(starting_number, int):
        raise TypeError('Starting number must be an integer')

    url = (
        'https://members-api.parliament.uk/api/Location/Constituencies?' +
        'skip=' + str(starting_number) + '&take=20'
    )

    r = requests.get(
        url,
        headers=headers
    )

    if save_logs:
        lo.log_details(
            logs_folder_path='logs',
            logs_file_name='constituency_search_status_codes.txt',
            message='Status code ' + str(r.status_code) + ' for url ' + url
        )

    if r.status_code == 200:
        r = r.json()
        return r
    else:
        return


def queryConstituencyAPI(
    constituency_id: int,
    headers: dict,
    save_logs: bool = True
) -> Optional[dict]:
    '''
    Requests details from the Constituency Parliament API

        Parameters:
            - constituency_id (int): Constituency id number
            - headers (str): Headers to use in the request
            - save_logs (bool): Whether to save logs to file

        Returns:
            - r (str): API results in JSON form if status code is 200,
            else None

        Notes:
            - 20 is the maximum number of results that can be pulled
            per query
            - This only returns current constituencies. Parliament APIs
            will return details on historic constituencies, but these
            aren't available via this endpoint
    '''
    if not isinstance(constituency_id, int):
        raise TypeError('Constituency ID must be an integer')

    url = (
        'https://members-api.parliament.uk/api/Location/' +
        'Constituency/' + str(constituency_id)
    )

    r = requests.get(
        url,
        headers=headers
    )

    if save_logs:
        lo.log_details(
            logs_folder_path='logs',
            logs_file_name='constituency_status_codes.txt',
            message='Status code ' + str(r.status_code) + ' for url ' + url
        )

    if r.status_code == 200:
        r = r.json()
        return r
    else:
        return


def extractConstituency(json: dict) -> list:
    '''
    Extracts constituency details from json returned from either
    the Constituency Parliament API or the Constituency Search
    Parliament API

        Parameters:
            - json (str): API results in JSON form

        Returns:
            - constituencies (list): List of dicts containing constituency details

        Notes:
            None
    '''

    constituencies = []

    dict = {}
    dict.update({
        'id': json['value']['id'],
        'name': json['value']['name'],
        'startDate': json['value']['startDate'],
        'endDate': json['value']['endDate'],
    })
    constituencies.append(dict)

    return constituencies


def queryStateOfTheParties(
    date: str,
    headers: dict,
    house: Literal[1, 2] = None,
    save_logs: bool = True
) -> Optional[dict]:
    '''
    Requests details from the State of the Parties Parliament API

        Parameters:
            - date (str): Date to query in YYYY-MM-DD format
            - headers (dict): Headers to use in the request
            - house (int): House to search (1 = Commons, 2 = Lords)
            - save_logs (bool): Whether to save logs to file

        Returns:
            - json (dict): API results in JSON form if status code is 200,
            else None

        Notes:
            - house is a required argument for the State of the Parties API,
            unlike for the Members Search API
    '''
    if house is None:
        raise ValueError('House must be specified')
    elif not isinstance(house, int):
        raise TypeError('House must be an integer')
    elif house not in [1, 2]:
        raise ValueError('House must be 1 (Commons) or 2 (Lords)')

    url = (
        'https://members-api.parliament.uk/api/parties/stateOfTheParties/' +
        str(house) +
        '/' +
        str(date)
    )

    r = requests.get(
        url,
        headers=headers
    )

    if save_logs:
        lo.log_details(
            logs_folder_path='logs',
            logs_file_name='state_of_the_parties_status_codes.txt',
            message='Status code ' + str(r.status_code) + ' for url ' + url
        )

    if r.status_code == 200:
        json = r.json()
        return json
    else:
        return


def extractStateOfTheParties(
    json: dict,
    date: datetime.date
) -> list:
    '''
    Extracts member details from json returned from the
    State of the Parties Parliament API

        Parameters:
            - json (dict): API results in JSON form
            - date (datetime): Date used in query

        Returns:
            - state_of_the_parties (list): List of dicts containing details of each party

        Notes:
            None
    '''

    state_of_the_parties = []

    for party in json['items']:

        dict = {}
        dict.update({
            'date': date,
            'male': party['value']['male'],
            'female': party['value']['female'],
            'nonBinary': party['value']['nonBinary'],
            'total': party['value']['total'],
            'partyId': party['value']['party']['id'],
            'partyName': party['value']['party']['name'],
            'partyAbbreviation': party['value']['party']['abbreviation'],
            'partyBackgroundColour': party['value']['party']['backgroundColour'],
            'partyForegroundColour': party['value']['party']['foregroundColour'],
            'partyIsLordsMainParty': party['value']['party']['isLordsMainParty'],
            'partyIsLordsSpiritualParty': party['value']['party']['isLordsSpiritualParty'],
            'partyGovernmentType': party['value']['party']['governmentType'],
            'partyIsIndependentParty': party['value']['party']['isIndependentParty'],
        })
        state_of_the_parties.append(dict)

    return state_of_the_parties
