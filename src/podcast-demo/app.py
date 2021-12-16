from datetime import datetime, timedelta
from chalice import Chalice
from chalice.app import NotFoundError
from chalicelib.rss.comparison import compare_feeds

from memory_profiler import profile

from chalicelib.rss.appenders import run_trust_mode
from chalicelib.rss.sqs_messages import *

app = Chalice(app_name="rss-poc")


PODCASTS_ROOT_PATH = "Florin/podcasts"


@app.lambda_function()
@profile
def trust_mode(event, context):
    previous_date, current_date = get_time_interval(days=7)
    print(f"Feed URL: {event['feed_url']}")
    run_trust_mode(event["feed_url"], previous_date, current_date, PODCASTS_ROOT_PATH)


@app.lambda_function()
def fetch_item(event, context):
    event["item"]["published"] = datetime.fromisoformat(event["item"]["published"])
    write_item(RSSItem(**event["item"]), event["prefix"])
    return event
    pass


def get_time_interval(**time_diff):
    current_date = datetime.now().astimezone()
    previous_date = current_date - timedelta(**time_diff)
    return previous_date, current_date
