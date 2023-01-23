import requests
import time
import boto3
import json
import datetime


api_helper = {
    "base_url": "https://aviation-edge.com/v2/public",
    "token": ""
}

STREAM_NAME = "episen-flight-data-stream"
REGION = "us-east-1"
DELIVERY_STREAM_NAME = "KDS-S3-85YtU"
STREAM_SCHEMA = "arn:aws:firehose:us-east-1:921510072347:deliverystream/KDS-S3-85YtU"

airlines = ["AS", "G4", "AA", "XP", "MX", "DL", "2D", "F9", "HA", "B6", "WN", "NK", "SY", "UA"]
airports = ["ATL", "LAX", "ORD", "DFW", "DEN", "JFK", "SFO", "SEA", "MCO", "LAS"]


def track_schedules():
    start_date = "01/23/23"
    date = datetime.datetime.strptime(start_date, "%m/%d/%y")
    while True:
        get_put_us_top_airports_data("departure", date=date)
        get_put_us_top_airports_data("arrival", date=date)
        date = date + datetime.timedelta(days=1)
        print(date)
        time.sleep(1)


def get_put_us_top_airports_data(type, date):
    us_airports_data = []
    for airport in airports:
        url = f"{api_helper.get('base_url')}/timetable?key={api_helper.get('token')}&iataCode={airport}&type={type}&date={date}"
        airport_data = make_request(url)
        data = {f"{airport}": airport_data}
        put_data_aws_stream(data)
        us_airports_data.append(data)
    return us_airports_data


def put_data_aws_stream(data):
    kinesis_client = boto3.client('kinesis', region_name=REGION)
    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey="partitionkey")


def make_request(endpoint):
    headers = {
        # "'Accept': 'application/vnd.github+json',
        # 'Authorization': f'Bearer {api_helper.get("token")}',
    }

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        return response.json()

    if "API rate limit exceeded" in response.text:
        while True:
            time.sleep(10)

            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                return response.json()

    print(response.text)
    return False