import requests


class MarketStactApiClient:
    def __init__(self, api_key_id: str):
        self.base_url = f"http://api.marketstack.com/v1/eod?access_key="
        if api_key_id is None:
            raise Exception("API key cannot be set to None.")
        self.api_key_id = api_key_id
    
    def get_eods(
            self, ticker: str, start_time: str, end_time: str
    ) -> dict:
        """
        Get the end of day data for a list of tickers.

        Args:
            stock_tickers: the tickers for the stock
            start_time: start time in isoformat
            end_time: end time in isoformat

        Returns:
            A eod dictionary

        Raises:
            Exception if response code is not 200.
        """
        url = f"{self.base_url}{self.api_key_id}"
        params = {
            "symbols":ticker,
            "date_from":start_time,
            "date_to":end_time
        }
        response = requests.get(url,params=params)
        if response.status_code == 200 and response.json().get("data") is not None: 
            return response.json().get("data")
        else:
            raise Exception(
                f"Failed to extract data from Marketstack API. Status Code: {response.status_code}. Response: {response.text}"
                )