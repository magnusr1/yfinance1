import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from datetime import datetime
from db_pool import get_connection, return_connection, close_pool

# Set up database connection
DB_URL = os.environ['DATABASE_URL']

def execute_query(query, params=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        return_connection(conn)

def fetch_latest_assets():
    # Use the materialized view instead of the full table
    query = """
    SELECT Platform, AssetName, TotalUSD, Timestamp
    FROM latest_asset_values
    """
    return execute_query(query)

def fetch_historical_data():
    # Add time-based filtering and use indexed columns
    query = """
    SELECT Platform, AssetName, TotalUSD, Timestamp
    FROM AssetsHistoricalValues
    WHERE Timestamp >= CURRENT_DATE - INTERVAL '90 days'
    ORDER BY Timestamp DESC
    """
    return execute_query(query)

def create_pie_chart(data):
    df = pd.DataFrame(data, columns=['Platform', 'AssetName', 'TotalUSD', 'Timestamp'])
    total_by_platform = df.groupby('Platform')['TotalUSD'].sum().reset_index()

    plt.figure(figsize=(10, 8))
    plt.pie(total_by_platform['TotalUSD'], labels=total_by_platform['Platform'], autopct='%1.1f%%')
    plt.title('Current Asset Distribution by Platform')
    plt.axis('equal')
    plt.savefig('asset_distribution_pie.png')
    plt.close()

def create_bar_chart(data):
    df = pd.DataFrame(data, columns=['Platform', 'AssetName', 'TotalUSD', 'Timestamp'])

    plt.figure(figsize=(12, 6))
    sns.barplot(x='Platform', y='TotalUSD', data=df)
    plt.title('Total USD Value by Platform')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('platform_value_bar.png')
    plt.close()

def create_time_series_chart(data):
    df = pd.DataFrame(data, columns=['Platform', 'AssetName', 'TotalUSD', 'Timestamp'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    fig = px.line(df, x='Timestamp', y='TotalUSD', color='Platform', title='Asset Value Over Time')
    fig.write_html('asset_value_time_series.html')

def main():
    try:
        latest_data = fetch_latest_assets()
        historical_data = fetch_historical_data()

        create_pie_chart(latest_data)
        create_bar_chart(latest_data)
        create_time_series_chart(historical_data)

        total_value = sum(row[2] for row in latest_data)
        print(f"Total current value of all assets: ${total_value:.2f}")
    finally:
        close_pool()

if __name__ == "__main__":
    main()
