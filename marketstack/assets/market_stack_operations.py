import pandas as pd
import json
import requests
from datetime import datetime
import os


def extract_tickers(api_key_id: str) -> pd.DataFrame:    #-> pd.DataFrame:
    current_date = datetime.today().strftime('%Y-%m-%d')
    url = f"https://api.marketstack.com/v1/tickers?access_key={api_key_id}"
    relative_path = '../data/tickers/' + "/market_tickers.csv"
    response = requests.get(url)
    print(response.status_code)

    json_response = json.loads(response.text)
    data = json_response['data']
    df = pd.json_normalize(data, meta=["symbol"])

    df.to_csv(relative_path, index=False)
    print(f"File saved to {relative_path}")

    return df


def extract_eod(api_key_id: str):
    current_date = datetime.today().strftime('%Y-%m-%d')
    source_path = '../data' + "/market_tickers.csv"

    url = f"https://api.marketstack.com/v1/eod?access_key={api_key_id}"

    destination_path = '../data/'

    df_symbols = pd.read_csv(source_path)
    df_first_five = df_symbols.head()

    for item in df_first_five["symbol"]:

        querystring = {
            "symbols": item
        }

        response = requests.get(url, params=querystring)

        if response.status_code == 200:
            json_response = json.loads(response.text)
            data = json_response['data']
            df = pd.json_normalize(data, meta=["symbol"])
            df_final = pd.concat([df])

            file_name = destination_path + item + "_eod_" + current_date + ".csv"
            df.to_csv(file_name, index=False)
            print(f"File saved to {file_name}")

        else:
            raise Exception(
                "Error ocurred: " + response.text
            )


def transform_data():
    pass
