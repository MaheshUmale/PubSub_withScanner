
# Import necessary modules
import asyncio
import json
import ssl

import pandas as pd
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict, MessageToJson
import configparser
import MarketDataFeed_pb2 as pb
import json

from jsonpath_ng import jsonpath, parse
import datetime

#todaysDateTime = datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')
todaysDateTime = datetime.datetime.now(datetime.UTC).strftime('%Y_%m_%d_%H_%M_%S')
def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response



def extract_key_value(json_data, key):
    """Extracts a specific key-value pair from a JSON data"""
    data = json.loads(json_data)
    value = data.get(key)
    return value
def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


def normalize_json(data: dict) -> dict:
    new_data = dict()
    for key, value in data.items():
        if not isinstance(value, dict):
            new_data[key] = value
        else:
            for k, v in value.items():
                new_data[key + "_" + k] = v

    return new_data


def generate_csv_data(data: dict) -> str:
    # Defining CSV columns in a list to maintain
    # the order
    csv_columns = data.keys()

    # Generate the first row of CSV
    csv_data = ",".join(csv_columns) + "\n"

    # Generate the single record present
    new_row = list()
    for col in csv_columns:
        new_row.append(str(data[col]))

        # Concatenate the record with the column information
    # in CSV format
    csv_data += ",".join(new_row) + "\n"

    return csv_data
async def fetch_market_data():
    """Fetch market data using WebSocket and print it."""
    # Create a configparser object
    config = configparser.ConfigParser()

    # Read the properties file
    config.read('my.properties')


    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()
    if config.has_option('DEFAULT', 'token'):
        token = config['DEFAULT']['token']
        configuration.access_token = token
    api_version = '2.0'
   

    # Get market data feed authorization
    response = get_market_data_feed_authorize(
        api_version, configuration)

    # Connect to the WebSocket with SSL context
    async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(0.2)  # Wait for 1 second

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "ltpc",
                "instrumentKeys": ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50","NSE_FO|35415","NSE_FO|35165","NSE_FO|35006","NSE_FO|35080"]
            }
        }

        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        # Continuously receive and decode data from WebSocket
        global todaysDateTime
        csvDateTime = datetime.datetime.now(datetime.UTC).strftime('%Y_%m_%d')
        with open("xxxupstox_WSS_output_"+todaysDateTime+".txt", "a") as f:
            while True:
                message = await websocket.recv()
                decoded_data = decode_protobuf(message)
                # Convert the decoded data to a dictionary
                data_dict = MessageToDict(decoded_data)
                #print(json.dumps(data_dict))
                #print(generate_csv_data(normalize_json(data_dict)))
                f.write(json.dumps(data_dict) + "\n")
                #getKEYS
                #$.feeds. * ~
                # jsonpath_expression_Keys = parse('$.feeds')
                # print("b4 find")
                # for key in jsonpath_expression_Keys.find( json.loads(str(json.dumps(data_dict)))):
                #     print(f'{key.value}')
                #     print("FIRST")
                #     symbolList = ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50", "NSE_FO|35415", "NSE_FO|35165", "NSE_FO|35006", "NSE_FO|35080"]
                #     for symbol in symbolList :
                #         value = extract_key_value(key.value,symbol)
                #         print("value")




                f.flush()


# try :
#
#     # Execute the function to fetch market data
#     asyncio.run(fetch_market_data())
# except exception as ex:
#     print(ex)
#     pass

async def main():
    try:
        result = await fetch_market_data()
        print(result)
    except Exception as e:
        print(f"Exception: {e}")

asyncio.run(main())