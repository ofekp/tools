from __future__ import print_function
import datetime
import pickle
import os.path
import sys
import re

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
day_format = "YYYYmmDD"
devs = {
    "Tomer": None,
    "Tal": None,
    "Lioz": None,
    "Hila": None,
    "Ofek": None,
    "Baruch": None,
    "Yuval C": None,
    "Shira": None,
    "Assaf K": None,
    "Guy": None,
    "Ayelet": None,
    "Omer D": None,
    "Oleg": None,
    "Dror P": None,
    "Omer B": None,
    "Or": None,
    "Anna": None,
    "Dror G": None,
}

sep = ' '


def get_blocked_users_on_date(date, users_constrains_map):
    users = set()
    for user in users_constrains_map:
        for blocked_dates in users_constrains_map[user]:
            if ((date >= blocked_dates[0]) &
                    (date < blocked_dates[1]) & (user in devs.keys())):
                users.add(user)
    return users


def add_users_constrains_for_calender(service, start_day, end_day, users_constrains_map, calender_name):
    calender_name_to_id = {d['summary']: d['id'] for d in service.calendarList().list().execute().get('items', [])}
    events_result = service.events().list(calendarId=calender_name_to_id[calender_name],
                                          timeMin=start_day + 'Z',
                                          timeMax=end_day + 'Z',
                                          singleEvents=True,
                                          orderBy='startTime').execute()
    events = events_result.get('items', [])
    if not events:
        print('No upcoming events found.')
    for event in events:
        start = get_specific_date(event, 'start')
        end = get_specific_date(event, 'end')
        user = event['summary'].strip().split(' ')[0]
        user = re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', user)).split()
        user = sep.join(user)
        if user not in users_constrains_map.keys():
            users_constrains_map[user] = []
        users_constrains_map[user] += [(convert_string_to_date(start, False), convert_string_to_date(end, False))]


def get_gemini_tlv_staff_constrains(service, start_day_raw, end_day_raw):
    start_day_full = convert_string_to_date(start_day_raw, True)
    end_day_full = convert_string_to_date(end_day_raw, True)

    users_constrains_map = {}
    all_dates_constrains = {}

    add_users_constrains_for_calender(service, start_day_full, end_day_full, users_constrains_map, 'Gemini TLV staff')
    add_users_constrains_for_calender(service, start_day_full, end_day_full, users_constrains_map,
                                      'Team\'s BS Constraints')

    start_day_part = convert_string_to_date(start_day_raw, False)
    end_day_part = convert_string_to_date(end_day_raw, False)

    while start_day_part <= end_day_part:
        blocked_users = get_blocked_users_on_date(start_day_part, users_constrains_map)
        all_dates_constrains[start_day_part.date()] = blocked_users
        start_day_part += datetime.timedelta(days=1)

    print(all_dates_constrains)


def get_specific_date(event, specific_date):
    date = event[specific_date].get('dateTime', event[specific_date].get('date'))
    date = date if len(date.split('T')) == 1 else date.split('T')[0]
    return date


def convert_string_to_date(date_str, with_iso_format):
    list = date_str.split('-')
    if with_iso_format:
        return datetime.datetime(int(list[0]), int(list[1]), int(list[2])).isoformat()
    else:
        return datetime.datetime(int(list[0]), int(list[1]), int(list[2]))


def main():
    """Shows basic usage of the Google Calendar API.
    Prints the start and name of the next 10 events on the user's calendar.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('calendar', 'v3', credentials=creds)
    # Call the Calendar API
    print('Getting the events')
    get_gemini_tlv_staff_constrains(service, sys.argv[1], sys.argv[2])


if __name__ == '__main__':
    main()
