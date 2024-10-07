import pandas as pd
import json
import requests
from datetime import datetime
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, Column, Integer, String, Table, MetaData
from sqlalchemy.dialects import postgresql
from dotenv import load_dotenv
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


def extract_eod(api_key_id: str, df_tickers: pd.DataFrame):
    #current_date = datetime.today().strftime('%Y-%m-%d')
    #source_path = '../data' + "/market_tickers.csv"

    url = f"https://api.marketstack.com/v1/eod?access_key={api_key_id}"
    '''
    destination_path = '../data/'
    df_symbols = pd.read_csv(source_path)
    df_first_five = df_symbols.head()
    '''
    df_symbols = df_tickers[['symbol']]
    df_list = []
    df_final = pd.DataFrame(None)
    for item in df_symbols:

        querystring = {
            "symbols": item,
            "date_from": "2024-09-01",
            "date_to": "2024-09-30"
        }

        response = requests.get(url, params=querystring)

        if response.status_code == 200:
            json_response = json.loads(response.text)
            data = json_response['data']
            df = pd.json_normalize(data, meta=["symbol"])
            df_list.append(df)

            '''
            file_name = destination_path + item + "_eod_" + current_date + ".csv"
            df.to_csv(file_name, index=False)
            print(f"File saved to {file_name}")
            '''
        else:
            raise Exception(
                "Error ocurred: " + response.text
            )

    for df in df_list:
        df_final = pd.concat([df_final, df], ignore_index=True)


    return df_final


def transform_data(df_tickers: pd.DataFrame, df_eod: pd.DataFrame) -> list[pd.DataFrame]:

    results_df_list = []

    """
        Ticker data transformation
    """
    df_tickers_mod = df_tickers[['name', 'symbol', 'stock_exchange.name', 'stock_exchange.acronym', 'stock_exchange.country', 'stock_exchange.city']]

    new_tickers_col = {'stock_exchange.name': 'stock_exchange_name',
                       'stock_exchange.acronym': 'stock_exchange_acronym',
                       'stock_exchange.country': 'stock_exchange_country',
                       'stock_exchange.city': 'stock_exchange_city'
                        }
    """
        EOD data transformation
    """
    df_eod_mod = df_eod[['open', 'high', 'low', 'close', 'volume', 'split_factor', 'dividend', 'date', 'symbol']]

    new_eod_col = {'symbol': 'eod_symbol',
                   'open': 'opening_price',
                   'high': 'highest_price',
                   'low': 'lowest_price',
                   'close': 'closing_price',
                   'volume': 'volume_transactions',
                   }
    df_tickers_renamed = df_tickers_mod.rename(columns=new_tickers_col)
    df_eod_renamed = df_eod_mod.rename(columns=new_eod_col)

    """
        EOD + TICKERS Data
    """
    df_merged = pd.merge(df_eod_renamed, df_tickers_mod, left_on='eod_symbol', right_on='symbol', how='inner')
    df_final = df_merged[['symbol', 'name', 'date', 'stock_exchange_name', 'stock_exchange_acronym', 'stock_exchange_country',
                           'stock_exchange_country''opening_price', 'highest_price', 'lowest_price', 'closing_price',
                           'volume_transactions', 'split_factor', 'dividend']]
    df_eod_tickers = df_final.drop('eod_symbol')

    return df_eod_tickers


def data_load(df_eod_tickers: pd.DataFrame) -> None:

    load_dotenv()
    user = os.environ.get("user")
    password = os.environ.get("password")
    host = os.environ.get("password")
    port = os.environ.get("host")
    database = os.environ.get("database")

    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )

    metadata = MetaData()
    table = Table(
        "eod_tickers",
        metadata,
        Column("symbol", String, primary_key=True),
        Column("Name", String, primary_key=False),
        Column("Date", String, primary_key=True),
        Column("stock_exchange_name", primary_key=True),
        Column("stock_exchange_acronym", primary_key=False),
        Column("stock_exchange_country", primary_key=False),
        Column("opening_price", float),
        Column("highest_price", float),
        Column("highest_price", float),
        Column("lowest_price", float),
        Column("closing_price", float),
        Column("volume_transactions", int),
        Column("split_factor", float),
        Column("dividend", float)
    )

    key_columns = [
        pk_column.name for pk_column in table.primary_key.columns.values()
    ]

    engine = create_engine(connection_url)
    metadata.create_all(engine)  # creates table if it does not exist
    insert_statement = postgresql.insert(table).values(df_eod_tickers)
    engine.execute(insert_statement)