from db_pool import get_connection, return_connection, close_pool
from datetime import datetime

def remove_entries_by_timestamp(timestamp_str="2024-11-12 07:00:39.41804"):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Delete from AssetsHistoricalValues
            cur.execute("""
                DELETE FROM AssetsHistoricalValues 
                WHERE Timestamp = %s::timestamp
            """, (timestamp_str,))
            assets_count = cur.rowcount

            # Delete from HistoricalRates
            cur.execute("""
                DELETE FROM HistoricalRates 
                WHERE Timestamp = %s::timestamp
            """, (timestamp_str,))
            rates_count = cur.rowcount

            conn.commit()
            print(f"Removed {assets_count} entries from AssetsHistoricalValues")
            print(f"Removed {rates_count} entries from HistoricalRates")

    finally:
        return_connection(conn)
        close_pool()

if __name__ == "__main__":
    remove_entries_by_timestamp()