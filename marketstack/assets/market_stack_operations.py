import pandas as pd
import json
import requests
from datetime import datetime

import os


def extract_tickers(api_key_id: str):    #-> pd.DataFrame:
    current_date = datetime.today().strftime('%Y-%m-%d')
    url = f"https://api.marketstack.com/v1/tickers?access_key={api_key_id}"
    relative_path = '../data' + "/market_tickers" + current_date + ".csv"
    response = requests.get(url)
    print(response.status_code)

    json_response = json.loads(response.text)
    data = json_response['data']
    df = pd.json_normalize(data, meta=["symbol"])
    df.to_csv(relative_path, index=False)
    print(f"File saved to {relative_path}")
    #return df


def extract_eod(api_key_id: str, date_from: str = None, date_to: str = None):
    current_date = datetime.today().strftime('%Y-%m-%d')
    source_path = '../data' + "/market_tickers.csv"

    print(source_path)

    if date_from is None:
        date_from = current_date

    if date_to is None:
        date_to = current_date

    url = f"https://api.marketstack.com/v1/eod?access_key={api_key_id}"
    symbols = []

    destination_path = '../data/'
    file_name = destination_path + "eod_" + date_from + "_to_" + date_to + ".csv"


    df_symbols = pd.read_csv(source_path)
    df_first_five = df_symbols.head()

    for item in df_first_five["symbol"]:
        symbols.append(item)

    querystring = {
        "symbols": symbols,
        "date_from": date_from,
        "date_to": date_to
    }

    response = requests.get(url, params=querystring)
    print(response.status_code)

    json_response = json.loads(response.text)
    data = json_response['data']
    df = pd.json_normalize(data, meta=["symbol"])

    df.to_csv(file_name, index=False)
    print(f"File saved to {file_name}")
    # return df
