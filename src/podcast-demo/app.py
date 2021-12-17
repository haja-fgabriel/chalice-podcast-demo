from datetime import datetime, timedelta
from chalice import Chalice

from memory_profiler import profile

from chalicelib.rss import RSSItem
from chalicelib.rss.appenders import run_trust_mode
from chalicelib.rss.writers import extract_content_and_write_item

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
    extract_content_and_write_item(RSSItem(**event["item"]), event["prefix"])
    return event
    pass


def get_time_interval(**time_diff):
    current_date = datetime.now().astimezone()
    previous_date = current_date - timedelta(**time_diff)
    return previous_date, current_date
