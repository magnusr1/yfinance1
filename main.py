import os
import psycopg2
import yfinance as yf
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Tuple, Optional
import logging
import requests
import jwt
from cryptography.hazmat.primitives import serialization
import secrets
import json
import time
from moralis import evm_api
from db_pool import get_connection, return_connection, close_pool
from functools import lru_cache
from db_logger import log_db_operation, db_logger
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

console = Console()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_URL = os.environ['DATABASE_URL']
HELIUS_API_KEY = os.environ['HELIUS_API_KEY']

# Coinbase constants
COINBASE_KEY_SECRET = """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIPSCtOyQV66JZT9UjlmS7KUfdiDCUikKOohmSYCBDOMLoAoGCCqGSM49
AwEHoUQDQgAEShs8Blo6xV6wyU/kHFtxY3RujIG1S8njDE07UwTjBItMKLvjtHKX
DfOKmpu48elM8J3MT81jjLvEz4/W65hIhQ==
-----END EC PRIVATE KEY-----
"""
COINBASE_KEY_NAME = os.environ['COINBASE_KEY_NAME']
COINBASE_REQUEST_HOST = "api.coinbase.com"

# Ethereum constants
MORALIS_API_KEY = os.environ['MORALIS_API_KEY']

# Add this price caching mechanism
class PriceCache:
    def __init__(self):
        self.cache = {}
        self.last_refresh = None
        
    def get(self, ticker):
        now = datetime.now()
        # Refresh cache if it's older than 2 hours or doesn't exist
        if self.last_refresh is None or now - self.last_refresh > timedelta(hours=2):
            self.cache.clear()
            self.last_refresh = now
        
        if ticker not in self.cache:
            self.cache[ticker] = get_latest_price(ticker)
        return self.cache[ticker]

price_cache = PriceCache()

# Coinbase functions
def build_jwt(uri):
    private_key_bytes = COINBASE_KEY_SECRET.encode('utf-8')
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None)
    jwt_payload = {
        'sub': COINBASE_KEY_NAME,
        'iss': "cdp",
        'nbf': int(time.time()),
        'exp': int(time.time()) + 120,
        'uri': uri,
    }
    jwt_token = jwt.encode(
        jwt_payload,
        private_key,
        algorithm='ES256',
        headers={'kid': COINBASE_KEY_NAME, 'nonce': secrets.token_hex()},
    )
    return jwt_token

def make_coinbase_api_request(method, path):
    uri = f"{method} {COINBASE_REQUEST_HOST}{path}"
    jwt_token = build_jwt(uri)
    url = f"https://{COINBASE_REQUEST_HOST}{path}"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    response = requests.request(method, url, headers=headers)
    return response

def process_coinbase_holdings(current_timestamp):
    # First request to get portfolios
    portfolios_response = make_coinbase_api_request("GET", "/api/v3/brokerage/portfolios")
    if portfolios_response.status_code != 200:
        logging.error(f"Error fetching Coinbase portfolios: {portfolios_response.status_code}")
        logging.error(portfolios_response.text)
        return

    portfolios_data = json.loads(portfolios_response.text)
    if not portfolios_data.get("portfolios"):
        logging.warning("No Coinbase portfolios found")
        return

    # Get the first portfolio UUID
    portfolio_uuid = portfolios_data["portfolios"][0]["uuid"]

    # Second request to get portfolio overview
    overview_response = make_coinbase_api_request("GET", f"/api/v3/brokerage/portfolios/{portfolio_uuid}")
    if overview_response.status_code == 200:
        overview_data = json.loads(overview_response.text)
        insert_coinbase_holdings(overview_data, current_timestamp)
    else:
        logging.error(f"Coinbase Portfolio Overview Response Status Code: {overview_response.status_code}")
        logging.error(f"Coinbase Portfolio Overview Response Content: {overview_response.text}")

def insert_coinbase_holdings(data, current_timestamp):
    portfolio = data['breakdown']['portfolio']
    positions = data['breakdown']['spot_positions']

    for position in positions:
        total_balance_fiat = Decimal(position['total_balance_fiat'])
        if total_balance_fiat > 10:
            asset = position['asset']
            amount = Decimal(position['total_balance_crypto'])
            native_price = total_balance_fiat / amount if amount != 0 else Decimal('0')

            insert_data = (
                "Coinbase",
                portfolio['type'],
                "Crypto",
                asset,
                f"{asset}-USD",
                amount,
                "USD",
                native_price,
                native_price,
                total_balance_fiat,
                total_balance_fiat,
                current_timestamp
            )
            insert_historical_value(insert_data)

# Database operations
def execute_query(query: str, params: tuple = None, fetch: bool = False) -> Optional[List[Tuple]]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            conn.commit()
    finally:
        return_connection(conn)

def create_table(table_name: str, table_schema: str):
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})"
    execute_query(query)
    logging.info(f"{table_name} table created or already exists.")

def ensure_unique_constraint(table_name: str, column_name: str):
    # Check if the constraint exists
    constraint_name = f"unique_{column_name.lower()}"
    query = f"""
    SELECT constraint_name 
    FROM information_schema.table_constraints 
    WHERE table_name = '{table_name.lower()}' 
    AND constraint_type = 'UNIQUE'
    AND constraint_name = '{constraint_name}'
    """
    if not execute_query(query, fetch=True):
        # If the constraint doesn't exist, add it
        try:
            query = f"""
            ALTER TABLE {table_name}
            ADD CONSTRAINT {constraint_name} UNIQUE ({column_name})
            """
            execute_query(query)
            logging.info(f"Added unique constraint to {column_name} column.")
        except psycopg2.errors.UniqueViolation:
            logging.warning(f"Unique constraint couldn't be added due to duplicate values. Cleaning up duplicates...")
            query = f"""
            DELETE FROM {table_name} a USING (
                SELECT MIN(ctid) as ctid, {column_name}
                FROM {table_name} 
                GROUP BY {column_name} HAVING COUNT(*) > 1
            ) b
            WHERE a.{column_name} = b.{column_name} 
            AND a.ctid <> b.ctid
            """
            execute_query(query)
            query = f"""
            ALTER TABLE {table_name}
            ADD CONSTRAINT {constraint_name} UNIQUE ({column_name})
            """
            execute_query(query)
            logging.info(f"Duplicates removed and unique constraint added.")
    else:
        logging.info(f"Unique constraint already exists on {column_name} column.")

# Data insertion
def insert_data(table_name: str, columns: str, values: tuple):
    placeholders = ', '.join(['%s'] * len(values))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    execute_query(query, values)

def upsert_data(table_name: str, columns: str, values: tuple, conflict_column: str):
    placeholders = ', '.join(['%s'] * len(values))
    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns.split(', ') if col != conflict_column])
    query = f"""
    INSERT INTO {table_name} ({columns})
    VALUES ({placeholders})
    ON CONFLICT ({conflict_column}) DO UPDATE SET
    {update_set}
    """
    execute_query(query, values)

# Data fetching
def fetch_data(query: str, params: tuple = None) -> List[Tuple]:
    return execute_query(query, params, fetch=True)

# Price fetching
def get_latest_price(ticker: str) -> Optional[float]:
    periods = ["1d", "5d"]
    
    for period in periods:
        try:
            stock = yf.Ticker(ticker)
            history = stock.history(period=period)
            if not history.empty:
                latest_price = history['Close'].iloc[-1]
                logging.info(f"Latest price for {ticker} (period={period}): {latest_price}")
                return float(latest_price)
            else:
                logging.warning(f"No price data found for {ticker} (period={period})")
        except Exception as e:
            logging.error(f"Error fetching price for {ticker} (period={period}): {str(e)}")
    
    logging.error(f"Failed to fetch price for {ticker} after trying multiple periods")
    return None

# Currency conversion
def get_usd_exchange_rate(currency: str) -> Optional[Decimal]:
    if currency == 'USD':
        return Decimal('1.0')
    
    ticker = fetch_data("SELECT Ticker FROM TrackedCurrenciesIndexes WHERE FromCurrency = %s AND ToCurrency = 'USD'", (currency,))
    if not ticker:
        logging.warning(f"No conversion ticker found for {currency} to USD")
        return None
    
    price = price_cache.get(ticker[0][0])  # Use cache instead of direct call
    if price is None:
        logging.error(f"Failed to fetch USD exchange rate for {currency}")
        return None
    return Decimal(str(price))

# Main operations
def setup_database():
    create_table("AssetsHistoricalValues", """
        ID SERIAL PRIMARY KEY,
        Platform VARCHAR(255),
        AccountWallet VARCHAR(255),
        AssetType VARCHAR(255),
        AssetName VARCHAR(255),
        Ticker VARCHAR(255),
        Amount DECIMAL,
        NativeCurrency VARCHAR(10),
        NativePrice DECIMAL,
        USDPrice DECIMAL,
        TotalNative DECIMAL,
        TotalUSD DECIMAL,
        Timestamp TIMESTAMP
    """)
    create_table("TrackedCurrenciesIndexes", """
        ID SERIAL PRIMARY KEY,
        IndexName VARCHAR(255),
        Ticker VARCHAR(255) UNIQUE,
        FromCurrency VARCHAR(10),
        ToCurrency VARCHAR(10)
    """)
    create_table("HistoricalRates", """
        ID SERIAL PRIMARY KEY,
        Ticker VARCHAR(255),
        Price DECIMAL,
        Timestamp TIMESTAMP
    """)
    ensure_unique_constraint("TrackedCurrenciesIndexes", "Ticker")
    
    # Enhanced indexing for historical queries
    index_queries = [
        """
        CREATE INDEX IF NOT EXISTS idx_assets_timestamp ON AssetsHistoricalValues(Timestamp DESC)
        INCLUDE (Platform, AssetName, TotalUSD)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_assets_platform_timestamp ON AssetsHistoricalValues(Platform, Timestamp DESC)
        INCLUDE (AssetName, TotalUSD)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_historical_rates_lookup ON HistoricalRates(Ticker, Timestamp DESC)
        INCLUDE (Price)
        """
    ]
    
    for query in index_queries:
        execute_query(query)
        db_logger.info(f"Created or verified index with query: {query}")
    
    # Create materialized view for latest values with a unique index
    materialized_view_query = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS latest_asset_values AS
    SELECT DISTINCT ON (Platform, AssetName) 
        Platform,
        AssetName,
        TotalUSD,
        Timestamp
    FROM AssetsHistoricalValues
    ORDER BY Platform, AssetName, Timestamp DESC;
    
    CREATE UNIQUE INDEX IF NOT EXISTS idx_latest_asset_values_unique 
    ON latest_asset_values(Platform, AssetName);
    """
    execute_query(materialized_view_query)
    
    # Create function to refresh materialized view
    refresh_function_query = """
    CREATE OR REPLACE FUNCTION refresh_latest_asset_values()
    RETURNS TRIGGER AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY latest_asset_values;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """
    execute_query(refresh_function_query)
    
    # Create trigger to refresh view
    trigger_query = """
    DROP TRIGGER IF EXISTS refresh_latest_asset_values_trigger ON AssetsHistoricalValues;
    CREATE TRIGGER refresh_latest_asset_values_trigger
    AFTER INSERT OR UPDATE ON AssetsHistoricalValues
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_latest_asset_values();
    """
    execute_query(trigger_query)

def insert_tracked_items():
    items = [
        ("USD/NOK", "NOK=X", "USD", "NOK"),
        ("EUR/USD", "EURUSD=X", "EUR", "USD"),
        ("SEK/USD", "SEKUSD=X", "SEK", "USD"),
        ("BTC", "BTC-USD", "BTC", "USD"),
        ("ETH", "ETH-USD", "ETH", "USD"),
        ("SOL", "SOL-USD", "SOL", "USD"),
        ("NASDAQ Composite", "^IXIC", "NASDAQ", "USD"),
    ]
    for item in items:
        upsert_data("TrackedCurrenciesIndexes", "IndexName, Ticker, FromCurrency, ToCurrency", item, "Ticker")
    logging.info("Tracked items inserted or updated in TrackedCurrenciesIndexes table.")

def get_sol_price():
    query = "SELECT Price FROM HistoricalRates WHERE Ticker = 'SOL-USD' ORDER BY Timestamp DESC LIMIT 1"
    result = fetch_data(query)
    if result:
        return Decimal(str(result[0][0]))
    else:
        logging.warning("SOL price not found in HistoricalRates. Using default value.")
        return Decimal('20')  # Default value if not found

def process_solana_wallet_holdings(wallet_address, alias, current_timestamp):
    logging.info(f"Processing holdings for Solana wallet: {alias} ({wallet_address})")
    
    get_assets_result = fetch_solana_wallet_holdings_get_assets(wallet_address)
    native_sol_balance = get_native_sol_balance(get_assets_result)
    
    search_assets_result = fetch_solana_wallet_holdings_search_assets(wallet_address)
    other_assets = get_other_assets(search_assets_result)
    
    all_assets = combine_and_filter_assets(native_sol_balance, other_assets)
    
    print_combined_results(alias, all_assets)
    
    # Prepare batch insert data
    insert_data_list = []
    for asset in all_assets:
        balance = Decimal(str(asset['balance']))
        total_price = Decimal(str(asset['total_price']))
        
        insert_data = (
            "Solana",
            wallet_address,
            "Crypto",
            asset['symbol'],
            f"{asset['symbol']}-USD",
            balance,
            "USD",
            (total_price / balance) if balance != 0 else Decimal('0'),
            (total_price / balance) if balance != 0 else Decimal('0'),
            total_price,
            total_price,
            current_timestamp
        )
        insert_data_list.append(insert_data)
    
    # Perform batch insert
    batch_insert_historical_values(insert_data_list)

def fetch_solana_wallets():
    query = "SELECT AccountWallet, Alias FROM CryptoWallets WHERE Platform = 'Solana'"
    return execute_query(query, fetch=True)

def fetch_solana_wallet_holdings_search_assets(wallet_address):
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "id": "helius-test",
        "method": "searchAssets",
        "params": {
            "ownerAddress": wallet_address,
            "tokenType": "all"
        }
    }
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data for wallet {wallet_address} using searchAssets: {str(e)}")
        return None

def fetch_solana_wallet_holdings_get_assets(wallet_address):
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "id": "helius-test",
        "method": "getAssetsByOwner",
        "params": {
            "ownerAddress": wallet_address,
            "displayOptions": {
                "showFungible": True,
                "showNativeBalance": True
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data for wallet {wallet_address} using getAssetsByOwner: {str(e)}")
        return None

def print_results(method, result, alias):
    print(f"\n--- Results for {method} (Wallet: {alias}) ---")
    if result and "result" in result:
        if method == "searchAssets":
            tokens = result["result"].get("items", [])
            for token in tokens:
                token_info = token.get("token_info", {})
                price_info = token_info.get("price_info", {})
                symbol = token_info.get("symbol", "N/A")
                balance = token_info.get("balance", 0)
                decimals = token_info.get("decimals", 0)
                total_price = Decimal(str(price_info.get("total_price", 0)))
                print(f"Token: {symbol}, Balance: {balance / (10 ** decimals)}, Total Price: ${total_price:.2f}")
        elif method == "getAssetsByOwner":
            assets = result["result"].get("items", [])
            native_balance = result["result"].get("nativeBalance", {}).get("lamports", 0)
            print(f"Native SOL Balance: {native_balance / 1e9:.9f} SOL")
            for asset in assets:
                token_info = asset.get("token_info", {})
                balance = token_info.get("balance", 0)
                decimals = token_info.get("decimals", 0)
                symbol = token_info.get("symbol", "N/A")
                print(f"Token: {symbol}, Balance: {balance / (10 ** decimals)}")
    else:
        print(f"No data found for wallet using {method}")
    print("---\n")

def get_native_sol_balance(result):
    if result and "result" in result:
        native_balance = Decimal(str(result["result"].get("nativeBalance", {}).get("lamports", 0)))
        sol_price = Decimal(str(get_sol_price()))
        return {
            "symbol": "SOL",
            "balance": native_balance / Decimal('1e9'),
            "total_price": (native_balance / Decimal('1e9')) * sol_price
        }
    return None

def get_other_assets(result):
    """
    Return a list of fungible assets aggregated by mint:
    [
      {"mint": str, "symbol": str, "balance": Decimal, "total_price": Decimal}
    ]
    """
    assets = []
    if result and "result" in result:
        items = result["result"].get("items", [])
        for item in items:
            token_info = item.get("token_info", {})
            price_info = token_info.get("price_info", {})
            # Prefer token_info.mint; fall back to item.id if needed
            mint = token_info.get("mint") or item.get("id")
            symbol = token_info.get("symbol", "N/A")
            decimals = int(token_info.get("decimals", 0) or 0)
            raw_balance = Decimal(str(token_info.get("balance", 0)))
            balance = (
                raw_balance / (Decimal(10) ** decimals)
                if decimals >= 0 else Decimal(0)
            )
            total_price = Decimal(str(price_info.get("total_price", 0)))
            if mint:
                assets.append({
                    "mint": mint,
                    "symbol": symbol,
                    "balance": balance,
                    "total_price": total_price,
                })

    # Collapse by mint
    by_mint = {}
    for asset in assets:
        key = asset["mint"]
        if key not in by_mint:
            by_mint[key] = asset.copy()
        else:
            by_mint[key]["balance"] += asset["balance"]
            by_mint[key]["total_price"] += asset["total_price"]

    return list(by_mint.values())

def combine_and_filter_assets(native_sol, other_assets):
    all_assets = [native_sol] + other_assets if native_sol else other_assets
    return [asset for asset in all_assets if asset["total_price"] > 10]

def print_combined_results(alias, assets):
    print(f"\n--- Combined Results for Wallet: {alias} ---")
    for asset in assets:
        print(f"Token: {asset['symbol']}, Balance: {asset['balance']:.6f}, Total Price: ${asset['total_price']:.2f}")
    print("---\n")

def insert_historical_value(data):
    query = """
    INSERT INTO AssetsHistoricalValues 
    (Platform, AccountWallet, AssetType, AssetName, Ticker, Amount, NativeCurrency, NativePrice, USDPrice, TotalNative, TotalUSD, Timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    execute_query(query, data)

def update_holdings_and_rates():
    current_timestamp = datetime.now()
    holdings = fetch_data("SELECT Platform, AccountWallet, AssetType, AssetName, Ticker, Amount, NativeCurrency FROM ManualHoldings")
    tracked_items = fetch_data("SELECT Ticker FROM TrackedCurrenciesIndexes")

    for holding in holdings:
        platform, account_wallet, asset_type, asset_name, ticker, amount, native_currency = holding
        price = get_latest_price(ticker)
        if price is not None:
            if platform == "Solana":
                usd_exchange_rate = Decimal('1.0')
                native_currency = "USD"
            else:
                usd_exchange_rate = get_usd_exchange_rate(native_currency)
            
            if usd_exchange_rate is None:
                logging.warning(f"Skipping insertion for {ticker} due to missing USD exchange rate")
                continue

            native_price = Decimal(str(price))
            usd_price = native_price * usd_exchange_rate
            total_native = Decimal(str(amount)) * native_price
            total_usd = Decimal(str(amount)) * usd_price

            insert_data = (
                platform,
                account_wallet,
                asset_type,
                asset_name,
                ticker,
                amount,
                native_currency,
                native_price,
                usd_price,
                total_native,
                total_usd,
                current_timestamp
            )
            insert_historical_value(insert_data)
        else:
            logging.warning(f"Failed to fetch price for {ticker}")

    for ticker in tracked_items:
        price = get_latest_price(ticker[0])
        if price is not None:
            query = "INSERT INTO HistoricalRates (ticker, price, timestamp) VALUES (%s, %s, %s)"
            execute_query(query, (ticker[0], price, current_timestamp))
        else:
            logging.warning(f"Failed to fetch price for {ticker[0]}")

    # Process Solana wallets
    solana_wallets = fetch_solana_wallets()
    for wallet_address, alias in solana_wallets:
        process_solana_wallet_holdings(wallet_address, alias, current_timestamp)

    # Process Ethereum wallets
    ethereum_wallets = fetch_ethereum_wallets()
    for wallet_address, alias in ethereum_wallets:
        process_ethereum_holdings(wallet_address, alias, current_timestamp)

    # Process Coinbase holdings
    process_coinbase_holdings(current_timestamp)

# Add this function to calculate total USD value
def calculate_total_values():
    query = """
    SELECT Platform, SUM(TotalUSD) 
    FROM AssetsHistoricalValues 
    WHERE Timestamp = (SELECT MAX(Timestamp) FROM AssetsHistoricalValues)
    GROUP BY Platform
    """
    results = fetch_data(query)
    
    usd_to_nok_rate = get_usd_to_nok_rate()
    platform_totals = {}
    total_usd = Decimal('0')
    
    for platform, usd_value in results:
        usd_value = Decimal(str(usd_value))
        nok_value = usd_value * usd_to_nok_rate
        platform_totals[platform] = {
            'USD': usd_value,
            'NOK': nok_value
        }
        total_usd += usd_value
    
    total_nok = total_usd * usd_to_nok_rate
    
    return platform_totals, total_usd, total_nok

# Ethereum functions
def get_native_balance(address):
    params = {
        "chain": "eth",
        "address": address
    }
    result = evm_api.balance.get_native_balance(
        api_key=MORALIS_API_KEY,
        params=params,
    )
    balance = Decimal(result['balance']) / Decimal(10**18)
    return balance

def get_token_price(token_address):
    params = {
        "chain": "eth",
        "include": "percent_change",
        "address": token_address
    }
    try:
        result = evm_api.token.get_token_price(
            api_key=MORALIS_API_KEY,
            params=params,
        )
        return Decimal(result.get('usdPrice', 0))
    except Exception:
        return Decimal(0)

def get_valuable_ethereum_assets(address):
    valuable_assets = []

    # Get native ETH balance
    eth_balance = get_native_balance(address)
    eth_price = get_token_price("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")  # WETH address
    eth_value = eth_balance * eth_price

    if eth_value > Decimal(10):
        valuable_assets.append({
            'Platform': "Ethereum",
            'AccountWallet': address,
            'Assettype': "Crypto",
            'Assetname': "Ethereum",
            'Ticker': "ETH-USD",
            'Amount': eth_balance,
            'Nativecurrency': "USD",
            'Nativeprice': eth_price
        })

    # Get ERC20 tokens
    params = {
        "chain": "eth",
        "address": address
    }
    result = evm_api.token.get_wallet_token_balances(
        api_key=MORALIS_API_KEY,
        params=params,
    )

    for token in result:
        try:
            token_address = token.get('token_address')
            name = token.get('name', 'Unknown')
            symbol = token.get('symbol', 'Unknown')
            balance = Decimal(token.get('balance', '0'))
            decimals = int(token.get('decimals', 0) or 0)
            formatted_balance = balance / Decimal(10**decimals)
            price = get_token_price(token_address)

            total_value = formatted_balance * price
            if total_value > Decimal(10):
                valuable_assets.append({
                    'Platform': "Ethereum",
                    'AccountWallet': address,
                    'Assettype': "Crypto",
                    'Assetname': name,
                    'Ticker': f"{symbol}-USD",
                    'Amount': formatted_balance,
                    'Nativecurrency': "USD",
                    'Nativeprice': price
                })
        except Exception as e:
            logging.error(f"Error processing token {token.get('symbol', 'Unknown')}: {str(e)}")
            continue

    return valuable_assets

def process_ethereum_holdings(address, alias, current_timestamp):
    logging.info(f"Processing holdings for Ethereum wallet: {alias} ({address})")
    valuable_assets = get_valuable_ethereum_assets(address)
    
    for asset in valuable_assets:
        insert_data = (
            asset['Platform'],
            asset['AccountWallet'],
            asset['Assettype'],
            asset['Assetname'],
            asset['Ticker'],
            asset['Amount'],
            asset['Nativecurrency'],
            asset['Nativeprice'],
            asset['Nativeprice'],
            asset['Amount'] * asset['Nativeprice'],
            asset['Amount'] * asset['Nativeprice'],
            current_timestamp
        )
        insert_historical_value(insert_data)

def fetch_ethereum_wallets():
    query = "SELECT AccountWallet, Alias FROM CryptoWallets WHERE Platform = 'Ethereum'"
    return execute_query(query, fetch=True)

def get_usd_to_nok_rate():
    rate = get_latest_price("NOK=X")
    if rate is None:
        logging.warning("Failed to fetch USD to NOK exchange rate. Using default rate of 10.")
        return Decimal('10')
    return Decimal(str(rate))

# Add this function
def batch_insert_historical_values(data_list):
    if not data_list:
        return
        
    placeholders = ','.join(['(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'] * len(data_list))
    query = f"""
    INSERT INTO AssetsHistoricalValues 
    (Platform, AccountWallet, AssetType, AssetName, Ticker, Amount, 
     NativeCurrency, NativePrice, USDPrice, TotalNative, TotalUSD, Timestamp)
    VALUES {placeholders}
    """
    # Flatten the data list
    flat_data = [item for tuple_data in data_list for item in tuple_data]
    execute_query(query, tuple(flat_data))

# Add batch operations for historical rates
@log_db_operation
def batch_insert_historical_rates(rates_data):
    if not rates_data:
        return
        
    placeholders = ','.join(['(%s, %s, %s)'] * len(rates_data))
    query = f"""
    INSERT INTO HistoricalRates (Ticker, Price, Timestamp)
    VALUES {placeholders}
    ON CONFLICT (Ticker, Timestamp) DO UPDATE 
    SET Price = EXCLUDED.Price
    """
    flat_data = [item for tuple_data in rates_data for item in tuple_data]
    execute_query(query, tuple(flat_data))

# Add this function for better console output
def print_asset_overview(platform_totals, total_usd, total_nok):
    console.print("\n[bold cyan]Asset Value Overview[/bold cyan]", justify="center")
    
    # Create table for platform values
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Platform", style="cyan")
    table.add_column("USD", justify="right", style="green")
    table.add_column("NOK", justify="right", style="yellow")
    
    for platform, values in platform_totals.items():
        table.add_row(
            platform,
            f"${values['USD']:,.2f}",
            f"kr {values['NOK']:,.2f}"
        )
    
    # Add totals row
    table.add_row(
        "[bold]Total[/bold]",
        f"[bold green]${total_usd:,.2f}[/bold green]",
        f"[bold yellow]kr {total_nok:,.2f}[/bold yellow]"
    )
    
    console.print(table)

# Modify the main function
def main():
    try:
        with console.status("[bold green]Setting up database...") as status:
            setup_database()
            status.update("[bold green]Inserting tracked items...")
            insert_tracked_items()
            status.update("[bold green]Updating holdings and rates...")
            update_holdings_and_rates()
            
            platform_totals, total_usd, total_nok = calculate_total_values()
            
            status.update("[bold green]Generating reports...")
            print_asset_overview(platform_totals, total_usd, total_nok)
            
            logging.info("Asset values calculated and printed.")
            logging.info("Ethereum holdings processed and inserted into the database.")
            logging.info("Coinbase holdings processed and inserted into the database.")
            
            # Print performance summary at the end
            from db_logger import print_performance_summary
            print_performance_summary()
            
            console.print("\n[bold green]✓[/bold green] Processing completed successfully!")
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}")
        logging.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        close_pool()

if __name__ == "__main__":
    main()
