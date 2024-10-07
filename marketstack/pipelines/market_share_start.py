from dotenv import load_dotenv
import os
from marketstack.assets.market_stack_operations import extract_tickers, extract_eod, transform_data, data_load


if __name__ == "__main__":
    load_dotenv()
    api_key_id = os.environ.get("marketstack_api_key")
    host = os.environ.get("server_name")

    database = os.environ.get("database_name")
    user = os.environ.get("user")
    password = os.environ.get("password")
    port = os.environ.get("port")

    df_tickers = extract_tickers(api_key_id)
    df_eod = extract_eod(api_key_id, df_tickers)

    """
        lOAD TABLES
    """

    data_load(user, password, host, port, database, table, transform_data(df_tickers, df_eod))




