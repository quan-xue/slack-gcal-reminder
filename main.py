import base64
from datetime import datetime, timedelta
import json
import os.path
from collections import defaultdict
from enum import Enum, unique
from typing import List

# import functions_framework
import pandas as pd
import pendulum
import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']


# https://developers.google.com/calendar/api/quickstart/python

# https://cloud.google.com/functions/docs/configuring/secrets

LOCAL_TZ = pendulum.timezone("Asia/Singapore")

@unique
class EventType(Enum):
    FullDay = "full"
    PartialDay = "partial"


class CalEvent:
    def __init__(self, gcal_event):
        self.link = gcal_event.get('htmlLink', None)
        self.summary = gcal_event.get('summary', None)
        self.location = gcal_event.get('location', None)
        self.meeting_link = gcal_event.get('hangoutLink', None)

        start = gcal_event['start']
        end = gcal_event['end']
        # only contains date = full day event
        if 'date' in start.keys():
            self.event_type = EventType.FullDay
            # formatted asyyyy-mm-dd
            self.start_dt_str = start['date']
            self.end_dt_str = end['date']
        else:
            self.event_type = EventType.PartialDay
            target_dt_fmt = "%Y-%m-%d %H:%M"
            self.start_dt_str = datetime.fromisoformat(start['dateTime']).strftime(target_dt_fmt)
            self.end_dt_str = datetime.fromisoformat(end['dateTime']).strftime(target_dt_fmt)

    def format_slack_msg_section(self):
        base_text = f":calendar: <{self.link} |*{self.summary}*> \n"
        timing_str = f"{self.start_dt_str} to {self.end_dt_str} "
        base_text += timing_str
        if self.location:
            base_text += f"`{self.location}` "
        if self.meeting_link:
            base_text += f"`{self.meeting_link}`"
        return base_text


def generate_test_creds():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=5000)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def generate_service_acct_creds():
    return service_account.Credentials.from_service_account_file('service_acct_credentials.json', scopes=SCOPES)


def read_config():
    """
    Calendar ids and where to post them.
    Expecting config.csv.
    Reads from second line onwards as first line assumed to be header.
    """
    fp = 'config.csv'
    df = pd.read_csv(fp)
    webhook_to_cals = defaultdict(list)
    for row in df.itertuples():
        cal_id = row.calendar_id
        webhook = row.webhook
        description = row.description  # for troubleshooting
        webhook_to_cals[webhook].append((cal_id, description))

    return webhook_to_cals


def get_cal_events(service, cal_id: str, start_date: str, end_date: str):
    events_result = service.events().list(
        calendarId=cal_id,
        timeMin=start_date,
        timeMax=end_date,
        singleEvents=True,
        orderBy='startTime'
    ).execute()
    events = events_result.get('items', [])
    print(f"Events found: {events}")
    return [CalEvent(event) for event in events]


def format_event_section_daily(cal_name: str, events: List[CalEvent]):
    cal_start = f"*{cal_name}* has "
    if not events:
        return [cal_start + f"no event today. _Ahhhh_ that empty schedule feeling 🏖️"]
    else:
        num_events = len(events)
        cal_starting_section = cal_start + f"_{num_events}_ events today."
        return [cal_starting_section] + [event.format_slack_msg_section() for event in events]


def get_start_block_weekly():
    return f"🤘to a new week"


def format_event_section_weekly(cal_name: str, events: List[CalEvent]):
    cal_start = f"*{cal_name}* has "
    if not events:
        return [cal_start + f"nothing planned this week."]
    else:
        num_events = len(events)
        cal_starting_section = cal_start + f"_{num_events}_ events this week."
        return [cal_starting_section] + [event.format_slack_msg_section() for event in events]


def get_cal_name(service, cal_id):
    calendar = service.calendars().get(calendarId=cal_id).execute()
    return calendar.get('summary', '')


def get_start_block(execution_dt):
    datetime_fmt = "%A, %-d %b %Y"
    execution_dt_str = execution_dt.strftime(datetime_fmt)
    if execution_dt.weekday() == 0:
        return f"*{execution_dt_str}* 🤘to a new week\n"
    return f"*{execution_dt_str}* :sunglasses:\n"


def get_daily_start_end(execution_dt: datetime):
    """Returns start and end for daily reminder"""
    start_dt = datetime(execution_dt.year, execution_dt.month, execution_dt.day, 0, 0, 0, tzinfo=LOCAL_TZ)
    end_dt = datetime(execution_dt.year, execution_dt.month, execution_dt.day, 23, 59, 59, tzinfo=LOCAL_TZ)

    return start_dt, end_dt


def get_weekly_start_end(execution_dt: datetime):
    """Returns start and end for daily reminder"""
    start_dt = datetime(execution_dt.year, execution_dt.month, execution_dt.day, 0, 0, 0, tzinfo=LOCAL_TZ)
    end_of_week = start_dt + timedelta(days=6)
    end_dt = datetime(end_of_week.year, end_of_week.month, end_of_week.day, 23, 59, 59, tzinfo=LOCAL_TZ)

    return start_dt, end_dt


def send_reminder(execution_dt: datetime):
    print(execution_dt)
    if TESTING_FLAG:
        creds = generate_test_creds()
    else:
        creds = generate_service_acct_creds()
    service = build('calendar', 'v3', credentials=creds)
    # try:
    # Call the Calendar API
    start_dt, end_dt = get_daily_start_end(execution_dt)
    print(f"Getting events from {start_dt} to {end_dt}")
    webhook_to_cals = read_config()
    for webhook, cal_info in webhook_to_cals.items():
        print(webhook, cal_info)
        all_cal_sections = [get_start_block(execution_dt)]
        for cal_id, description in cal_info:
            print(f"Getting events for {description}")
            cal_name = get_cal_name(service, cal_id)
            if execution_dt.weekday() == 0:
                start_of_week_dt, end_of_week_dt = get_weekly_start_end(execution_dt)
                week_events = get_cal_events(service, cal_id, start_of_week_dt.isoformat(),
                                             end_of_week_dt.isoformat())
                weekly_cal_section = format_event_section_weekly(cal_name,  week_events)
                all_cal_sections += weekly_cal_section

            events = get_cal_events(service, cal_id, start_dt.isoformat(), end_dt.isoformat())
            cal_section = format_event_section_daily(cal_name, events)
            all_cal_sections += cal_section
        print("all cal sections: ", all_cal_sections)
        slack_msg = {"text": "\n\n".join(all_cal_sections)}
        print("Formatted slack message: ", slack_msg)
        response = requests.post(url=webhook, data=json.dumps(slack_msg))
        print("Post response: ", response)

    # except Exception as error:
    #     print('An error occurred: %s' % error)


# Triggered from a message on a Cloud Pub/Sub topic.
# @functions_framework.cloud_event
def main(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(cloud_event.data["message"]["data"]))
    sys_time_in_utc = datetime.now()
    sys_time_in_sg = sys_time_in_utc + timedelta(hours=8)
    send_reminder(sys_time_in_sg)


TESTING_FLAG = False


if __name__ == '__main__':
    dt = datetime.now()
    # dt = datetime(2023, 1, 9)
    send_reminder(dt)
