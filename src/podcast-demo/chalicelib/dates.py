from datetime import datetime
from zoneinfo import ZoneInfo
from json import JSONEncoder, JSONDecoder


class DateWithTimezoneEncoder(JSONEncoder):
    def default(self, date):
        if isinstance(date, datetime):
            return date.isoformat()


def parse_date_with_timezone(datestamp):
    timezone = datestamp[-3:]
    tzinfo = ZoneInfo(timezone) if timezone != "PST" else ZoneInfo("US/Pacific")
    return datetime.strptime(datestamp[:-4], "%a, %d %b %Y %H:%M:%S").replace(tzinfo=tzinfo)


def parse_s3_date(date):
    return datetime.strptime(date, "%Y%m%d_%H%M")


def unparse_s3_date(s3_date):
    return s3_date.strftime("%Y%m%d_%H%M")
