import requests
import json
import pandas as pd


class MarketStackApiClient:


    def __init__(self, api_key_id: str):
        self.base_url = "https://api.marketstack.com/v1/"

        if api_key_id is None:
            raise ValueError("API key cannot be None. Please provide a valid API key.")

        self.api_key_id = api_key_id


    '''
    def get_tickers(self, api_key_id: str) -> pd.DataFrame:

        url = f"https://api.marketstack.com/v1/tickers?access_key={api_key_id}"

        response = requests.get(url)
        if response.status_code == 200 and response.json().get("trades") is not None:
            return response.json()
        else:
            raise Exception(
                f"Failed to extract data from Alpaca API. Status Code: {response.status_code}. Response: {response.text}"
            )
    '''
