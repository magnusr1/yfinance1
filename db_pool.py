from psycopg2 import pool
import os

# Create connection pool
connection_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=os.environ['DATABASE_URL']
)

def get_connection():
    return connection_pool.getconn()

def return_connection(conn):
    connection_pool.putconn(conn)

def close_pool():
    connection_pool.closeall()
