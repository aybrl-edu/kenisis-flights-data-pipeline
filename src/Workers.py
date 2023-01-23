import requests
import time
import boto3
import json

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


def generate_stream():
    # US Airlines Flights
    put_data_aws_stream(get_us_airlines_flights())
    # US Airlines Routes
    get_put_us_airlines_routes()
    # US Top Airports Departures
    get_put_us_top_airports_data("departure")
    # US Top Airports Arrivals
    get_put_us_top_airports_data("arrival")


def get_us_airlines_flights():
    us_airlines_flights = []
    for airline in airlines:
        url = f"{api_helper.get('base_url')}/flights?key={api_helper.get('token')}&airlineIata={airline}"
        flight_data = make_request(url)
        data = {f"{airline}": flight_data}
        print(data)
        us_airlines_flights.append(data)
    return us_airlines_flights


def get_put_us_airlines_routes():
    us_airlines_routes = []
    for airline in airlines:
        url = f"{api_helper.get('base_url')}/routes?key={api_helper.get('token')}&airlineIata={airline}"
        route_data = make_request(url)
        data = {f"{airline}": route_data}
        print(data)
        put_data_aws_stream(data)
        us_airlines_routes.append(data)
    return us_airlines_routes


def get_put_us_top_airports_data(type):
    us_airports_data = []
    for airport in airports:
        url = f"{api_helper.get('base_url')}/timetable?key={api_helper.get('token')}&iataCode={airport}&type={type}"
        airport_data = make_request(url)
        data = {f"{airport}": airport_data}
        print(data)
        us_airports_data.append(data)
    return us_airports_data


def put_data_aws_stream(data):
    kinesis_client = boto3.client('kinesis', region_name=REGION)
    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey="partitionkey")


# Helpers
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
