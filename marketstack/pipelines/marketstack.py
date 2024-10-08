from dotenv import load_dotenv
import os
from jinja2 import Environment, FileSystemLoader
from marketstack.connectors.marketstack import MarketStactApiClient
from marketstack.connectors.postgresql import PostgreSqlClient
from marketstack.assets.marketstack import (
    extract_eods, 
    extract_tickers, 
    transform, 
    load,
)
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from datetime import datetime
import yaml 
from pathlib import Path
import schedule
import time
from marketstack.assets.pipeline_logging import PipelineLogging
from marketstack.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from marketstack.assets.transform import execute_template_sql


if __name__ == "__main__":
    # set up environment variables 
    load_dotenv()
    API_KEY_ID = os.environ.get("API_KEY_ID")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            config = pipeline_config.get("config")
            PIPELINE_NAME = pipeline_config.get("name")
            TARGET_TABLE = config.get("target_table")
            INCREMENTAL_EXTRACT_COLUMN = config.get("incremental_extract_column")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )
    

    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_config.get("name"),
        log_folder_path=config.get("log_folder_path"),
    )
    # set up environment variables
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=PIPELINE_NAME,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    try:
        metadata_logger.log()  # log start
        pipeline_logging.logger.info("Starting pipeline run")
        pipeline_logging.logger.info("Getting pipeline environment variables")
        API_KEY_ID = os.environ.get("API_KEY_ID")
        API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
        DB_USERNAME = os.environ.get("DB_USERNAME")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        SERVER_NAME = os.environ.get("SERVER_NAME")
        DATABASE_NAME = os.environ.get("DATABASE_NAME")
        PORT = os.environ.get("PORT")

        pipeline_logging.logger.info("Creating Marketstack API client")
        #extract
        marketstack_api_client = MarketStactApiClient(api_key_id=API_KEY_ID)
        postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
        )

        extract_from_time = postgresql_client.get_latest_timestamp(
            table=TARGET_TABLE,
            column=INCREMENTAL_EXTRACT_COLUMN,
            )
        extract_until_time = datetime.today().strftime('%Y-%m-%d')

        df_eods = extract_eods(
            MarketStactApiClient=marketstack_api_client,
            ticker_list="marketstack/data/marketstack/tickers.csv",
            start_time = extract_from_time,
            end_time = extract_until_time
        )
        df_tickers = extract_tickers(
            tickers_reference_path = "marketstack/data/marketstack/tickers.csv"
        )

        #transform
        df_transformed = transform(df_eod_data=df_eods, df_tickers=df_tickers)

        #load
        metadata = MetaData()
        target_table = TARGET_TABLE
        table = Table(
            target_table,
            metadata,
            Column("unique_primary_key", Integer, primary_key=True),
            Column("open", Float),
            Column("close", Float),
            Column("volume", Float),
            Column("dividend", Float),
            Column("ticker", String),
            Column("timestamp", String),
            Column("ticker_name", String),
            Column("stock_exchange", String),
            Column("stock_exchange_acronym", String),
            Column("stock_exchange_country", String),
            Column("stock_exchange_city", String),
        )
        load(
            df=df_transformed,
            postgresql_client=postgresql_client,
            table=table,
            metadata=metadata,
            load_method="upsert",
        )

        pipeline_logging.logger.info("run post load transformation logic")
        #second transform
        transform_template_environment = Environment(
            loader=FileSystemLoader("marketstack/assets/sql/transform")
        )
        execute_template_sql(
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
            table_name="tech_firm_performance")

        pipeline_logging.logger.info("Pipeline run successful")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # log end
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
