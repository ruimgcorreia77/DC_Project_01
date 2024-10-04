import pandas as pd
from marketstack.connectors.marketstack import MarketStactApiClient
from marketstack.connectors.postgresql import PostgreSqlClient
from pathlib import Path
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, select, func # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable 
from sqlalchemy.orm import Session
#https://docs.sqlalchemy.org/en/20/core/exceptions.html#sqlalchemy.exc.NoSuchTableError
from sqlalchemy.exc import NoSuchTableError
"""
put the get_latest_timestamp function in assets and build like the load function in Weather.py

"""

def extract_eods(
    MarketStactApiClient: MarketStactApiClient, ticker_list: Path, start_time: str, end_time: str
) -> pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities.
    """
    df_tickers = pd.read_csv(ticker_list)
    eod_data = []
    for ticker in df_tickers["ticker"]:
        eod_data.extend(
            MarketStactApiClient.get_eods(
                ticker=ticker,
                start_time=start_time,
                end_time=end_time
            )
        )

    df_eod_data = pd.json_normalize(eod_data)
    return df_eod_data


def extract_tickers(tickers_reference_path: Path) -> pd.DataFrame:
    """Extracts ticker exchange data from the tickers file"""
    df_tickers = pd.read_csv(tickers_reference_path)
    return df_tickers

def transform(df_eod_data: pd.DataFrame, df_tickers: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'
    #rename join column to match that in tickers csv file. rename date column to more meaningful timestamp column
    df_eod_data_renamed = df_eod_data.rename(
        columns={"symbol": "ticker","date": "timestamp"}
    )
    df_merged = pd.merge(left=df_eod_data_renamed, right=df_tickers, on=["ticker"])
    df_merged["unique_primary_key"] = df_merged["timestamp"].astype(str) + df_merged["ticker"].astype(str)  
    # convert ISO-8601 format column to datetime
    df_merged["timestamp"] = pd.to_datetime(df_merged["timestamp"])
    #drop unnecessary colunns
    df_eods = df_merged.drop(columns=["exchange","high","low","adj_high","adj_low","adj_close","adj_open","adj_volume","split_factor"])
    df_eods_transformed = df_eods.set_index(["unique_primary_key"])
    return df_eods_transformed
    

def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )




