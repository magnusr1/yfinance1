import os
import psycopg2
import yfinance as yf
from datetime import datetime
from decimal import Decimal
from typing import List, Tuple, Optional
import logging
import requests

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_URL = os.environ['DATABASE_URL']
HELIUS_API_KEY = os.environ['HELIUS_API_KEY']

# Database operations
def execute_query(query: str, params: tuple = None, fetch: bool = False) -> Optional[List[Tuple]]:
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            conn.commit()

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
    
    price = get_latest_price(ticker[0][0])
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
    
    # Insert assets into AssetsHistoricalValues
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
        insert_historical_value(insert_data)

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
    assets = []
    if result and "result" in result:
        tokens = result["result"].get("items", [])
        for token in tokens:
            token_info = token.get("token_info", {})
            price_info = token_info.get("price_info", {})
            symbol = token_info.get("symbol", "N/A")
            balance = Decimal(str(token_info.get("balance", 0)))
            decimals = token_info.get("decimals", 0)
            total_price = Decimal(str(price_info.get("total_price", 0)))
            assets.append({
                "symbol": symbol,
                "balance": balance / (10 ** decimals),
                "total_price": total_price
            })
    return assets

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

# Add this function to calculate total USD value
def calculate_total_usd_value():
    query = """
    SELECT SUM(TotalUSD) 
    FROM AssetsHistoricalValues 
    WHERE Timestamp = (SELECT MAX(Timestamp) FROM AssetsHistoricalValues)
    """
    result = fetch_data(query)
    if result and result[0][0]:
        total_usd = Decimal(str(result[0][0]))
        logging.info(f"Total USD value of all assets: ${total_usd:.2f}")
        return total_usd
    else:
        logging.warning("Unable to calculate total USD value")
        return Decimal('0')

def main():
    setup_database()
    insert_tracked_items()
    update_holdings_and_rates()
    total_usd_value = calculate_total_usd_value()
    print(f"Total USD value of all assets: ${total_usd_value:.2f}")

if __name__ == "__main__":
    main()