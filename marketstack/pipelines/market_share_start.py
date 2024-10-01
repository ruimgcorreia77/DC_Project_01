from dotenv import load_dotenv
import os
from marketstack.assets.market_stack_operations import extract_tickers, extract_eod

if __name__ == "__main__":
    load_dotenv()
    api_key_id = os.environ.get("marketstack_api_key")

    extract_tickers(api_key_id)
    extract_eod(api_key_id, date_from="2024-09-01", date_to="2024-09-30")





