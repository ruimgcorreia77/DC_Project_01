CREATE TABLE tickers_eod (
    opening_price NUMERIC(12, 4),  -- Support for large numbers with up to 4 decimal places
    highest_price NUMERIC(12, 4),
    lowest_price NUMERIC(12, 4),
    closing_price NUMERIC(12, 4),
    volume_transactions BIGINT,  -- Volume might be a large number
    split_factor NUMERIC(6, 4),  -- Split factors usually have decimals
    dividend NUMERIC(10, 4),  -- Support for dividend numbers
    symbol VARCHAR(10) NOT NULL,  -- Ticker symbol
    name VARCHAR(255),  -- Name of the company/asset
    date DATE NOT NULL,  -- Date for the EOD data
    stock_exchange_name VARCHAR(255) NOT NULL,  -- Stock Exchange Name
    stock_exchange_acronym VARCHAR(10),  -- Acronym of the stock exchange
    stock_exchange_country VARCHAR(100),  -- Country where the stock exchange is located
    stock_exchange_city VARCHAR(100),  -- City of the stock exchange
    PRIMARY KEY (symbol, date)  -- Composite primary key of symbol + date
);

CREATE TABLE tickers_kpis (
    symbol VARCHAR(10) NOT NULL,  -- Ticker symbol
    name VARCHAR(255),  -- Name of the company/asset
    total_growth NUMERIC(10, 4),  -- Total growth (can be a percentage or raw value)
    average_growth NUMERIC(10, 4),  -- Average growth (e.g., over a period of time)
    last_quarter_growth NUMERIC(10, 4),  -- Growth during the last quarter
    stock_exchange_name VARCHAR(255) NOT NULL,  -- Stock Exchange Name
    stock_exchange_acronym VARCHAR(10),  -- Acronym of the stock exchange
    stock_exchange_country VARCHAR(100),  -- Country where the stock exchange is located
    stock_exchange_city VARCHAR(100),  -- City of the stock exchange
    PRIMARY KEY (symbol)  -- Unique symbol for each company (assuming one row per company)
);