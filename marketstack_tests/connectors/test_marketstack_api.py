from dotenv import load_dotenv
from marketstack.connectors.marketstack import MarketStactApiClient
import os
import pytest
import pandas as pd


@pytest.fixture
def setup():
    load_dotenv()


def test_marketstack_client_get_eods(setup):
    API_KEY_ID = os.environ.get("API_KEY_ID")
    marketstack_api_client = MarketStactApiClient(api_key_id=API_KEY_ID)
    data = marketstack_api_client.get_eods(ticker="MSFT", start_time="2024-10-01",
    end_time="2024-10-04"),
    assert type(data) == tuple
    assert len(data) > 0