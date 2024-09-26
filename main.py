import base64
import json
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum, unique
from typing import List, Dict
from dataclasses import dataclass

#todo: comment out when testing
import functions_framework
import pandas as pd
import pendulum
import requests
from google.oauth2 import service_account
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
        target_dt_fmt = "%-d %b *%H:%M*"
        # only contains date = full day event
        if 'date' in start.keys():
            self.event_type = EventType.FullDay
            # full day event ends at 00:00 next day
            # formatted as yyyy-mm-dd
            self.start_dt = datetime.fromisoformat(start['date']).replace(hour=0, minute=0)
            self.end_dt = (datetime.fromisoformat(end['date']) - timedelta(days=1)).replace(hour=23, minute=59)
        else:
            self.event_type = EventType.PartialDay
            self.start_dt = datetime.fromisoformat(start['dateTime'])
            self.end_dt = datetime.fromisoformat(end['dateTime'])

        self.start_dt = self.start_dt.replace(tzinfo=LOCAL_TZ)
        self.end_dt = self.end_dt.replace(tzinfo=LOCAL_TZ)
        self.start_dt_str = self.start_dt.strftime(target_dt_fmt)
        self.end_dt_str = self.end_dt.strftime(target_dt_fmt)

    def format_slack_msg_section(self):
        base_text = f":calendar: <{self.link} |*{self.summary}*> \n"
        timing_str = f"{self.start_dt_str} to {self.end_dt_str} "
        base_text += timing_str
        if self.location:
            base_text += f"`{self.location}` "
        if self.meeting_link:
            base_text += f"`{self.meeting_link}`"
        return base_text


def generate_service_acct_creds():
    return service_account.Credentials.from_service_account_file('service_acct_credentials.json', scopes=SCOPES)


@dataclass
class CalConfig:
    calendar_id: str
    description: str
    weekend_ping: bool
    zero_report: bool
    weekly_report: bool


def read_config() -> Dict[str, List[CalConfig]]:
    """
    Calendar ids and where to post them.
    Expecting config.csv.
    Reads from second line onwards as first line assumed to be header.
    """
    fp = 'config.csv'
    df = pd.read_csv(fp)
    webhook_to_cals = defaultdict(list)
    for row in df.itertuples():
        if row.is_active:
            webhook_to_cals[row.webhook].append(
                CalConfig(
                    calendar_id=row.calendar_id,
                    description=row.description,
                    weekend_ping=row.weekend_ping,
                    zero_report=row.zero_report,
                    weekly_report=row.weekly_report
                )
            )

    return webhook_to_cals


def fullday_events_end_correction(events: List[CalEvent], start_dt: datetime, end_dt: datetime) -> List[CalEvent]:
    return [event for event in events if (start_dt <= event.end_dt and event.start_dt <= end_dt)]


def get_cal_events(service, cal_id: str, start_dt: datetime, end_dt: datetime) -> List[CalEvent]:
    events_result = service.events().list(
        calendarId=cal_id,
        timeMin=start_dt.isoformat(),
        timeMax=end_dt.isoformat(),
        singleEvents=True,
        orderBy='startTime'
    ).execute()
    events = events_result.get('items', [])
    print(f"Events found: {events}")
    cal_events = [CalEvent(event) for event in events]
    return fullday_events_end_correction(cal_events, start_dt, end_dt)


def format_event_section_daily(cal_name: str, events: List[CalEvent], zero_report: bool) -> List[str]:
    cal_start = f"*{cal_name}* has "
    if not events:
        return [cal_start + f"no event today. _Ahhhh_ that empty schedule feeling 🏖️"] if zero_report else []
    else:
        num_events = len(events)
        cal_starting_section = cal_start + f"_{num_events}_ events today."
        return [cal_starting_section] + [event.format_slack_msg_section() for event in events]


def format_event_section_weekly(cal_name: str, events: List[CalEvent], zero_report: bool) -> List[str]:
    cal_start = f"*{cal_name}* has "
    if not events:
        return [cal_start + f"nothing planned this week."] if zero_report else []
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
    creds = generate_service_acct_creds()
    service = build('calendar', 'v3', credentials=creds)
    start_dt, end_dt = get_daily_start_end(execution_dt)
    print(f"Getting events from {start_dt} to {end_dt}")
    webhook_to_cals = read_config()
    is_weekend = execution_dt.weekday() > 4
    for webhook, cal_configs in webhook_to_cals.items():
        print(webhook, cal_configs)
        try:
            all_cal_sections = []
            if is_weekend:
                cal_configs = [config for config in cal_configs if config.weekend_ping]
            for config in cal_configs:
                print(f"Getting events for {config.description}")
                cal_name = get_cal_name(service, config.calendar_id)
                if execution_dt.weekday() == 0 and config.weekly_report:
                    start_of_week_dt, end_of_week_dt = get_weekly_start_end(execution_dt)
                    week_events = get_cal_events(service, config.calendar_id, start_of_week_dt,
                                                 end_of_week_dt)
                    weekly_cal_section = format_event_section_weekly(cal_name, week_events, config.zero_report)
                    all_cal_sections += weekly_cal_section

                events = get_cal_events(service, config.calendar_id, start_dt, end_dt)
                cal_section = format_event_section_daily(cal_name, events, config.zero_report)
                all_cal_sections += cal_section
            print("all cal sections: ", all_cal_sections)
            # no ping to webhook if no event
            if not all_cal_sections:
                continue
            all_cal_sections.insert(0, get_start_block(execution_dt))
            slack_msg = {"text": "\n\n".join(all_cal_sections)}
            print("Formatted slack message: ", slack_msg)
            response = requests.post(url=webhook, data=json.dumps(slack_msg))
            print("Post response: ", response)
        except Exception as e:
            print(e)


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event # todo: comment out when testing
def main(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(cloud_event.data["message"]["data"]))
    sys_time_in_utc = datetime.now()
    sys_time_in_sg = sys_time_in_utc + timedelta(hours=8)
    send_reminder(sys_time_in_sg)


if __name__ == '__main__':
    dt = datetime.now()
    # dt = datetime(2023, 1, 22)
    send_reminder(dt)
