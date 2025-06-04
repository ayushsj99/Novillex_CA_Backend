# db.py
from sqlalchemy import (
    create_engine, Table, Column, Integer, String, Date, Float,
    MetaData, ForeignKey, TIMESTAMP, Text, DateTime
)
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# === Fetch env vars ===
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Prefer assembled connection string if all parts are set
if USER and PASSWORD and HOST and PORT and DBNAME:
    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"
else:
    # Fallback to full DATABASE_URL (used in Render/Supabase)
    DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL is not set. Please configure your environment variables.")

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Optional test connection
try:
    with engine.connect() as connection:
        print("✅ Database connection successful!")
except Exception as e:
    print(f"❌ Failed to connect to the database: {e}")

# Metadata object for schema
metadata = MetaData()

# === Table Definitions ===

users = Table('users', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('username', String, unique=True, nullable=False),
    Column('created_at', TIMESTAMP, default=datetime.now)
)

transactions = Table('transactions', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('date', Date),
    Column('transaction_id', Text),
    Column('particulars', Text),
    Column('debit_amount', Float),
    Column('credit_amount', Float),
    Column('balance_amount', Float),
    Column('type', String),
    Column('created_at', TIMESTAMP, default=datetime.now)
)

user_table_hashes = Table('user_table_hashes', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('table_name', Text, nullable=False),
    Column('hash', Text, nullable=False),
    Column('created_at', DateTime, default=datetime.now)
)

user_table_metadata = Table('user_table_metadata', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('table_hash_id', Integer, ForeignKey('user_table_hashes.id')),
    Column('bank_name', Text),
    Column('account_number', Text),
    Column('report_period', Text),
    Column('opening_balance', Float),
    Column('opening_balance_type', String),
    Column('closing_balance', Float),
    Column('closing_balance_type', String),
    Column('transaction_period', Text),
    Column('account_holder_name', Text),
    Column('metadata_hash', Text, nullable=False),
    Column('created_at', DateTime, default=datetime.now)
)

# === Table creation function ===
def create_tables():
    metadata.create_all(engine)




# # db_connection.py
# from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, MetaData, ForeignKey, TIMESTAMP, Text, DateTime
# from datetime import datetime
# import sqlalchemy as sa
# import os
# from dotenv import load_dotenv

# load_dotenv()  # Load environment variables from .env file

# # PostgreSQL connection URL — update these as per your local setup
# DATABASE_URL = os.getenv('SUPABASE_DATABASE_URL')


# engine = create_engine(DATABASE_URL)
# metadata = MetaData()

# # User table
# users = Table('users', metadata,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('username', String, unique=True, nullable=False),
#     Column('created_at', TIMESTAMP, default=datetime.now)
# )

# # Transactions table (base schema if needed globally)
# transactions = Table('transactions', metadata,
#     Column('id', Integer, primary_key=True),
#     Column('user_id', Integer, ForeignKey('users.id')),
#     Column('date', Date),
#     Column('transaction_id', Text),
#     Column('particulars', Text),
#     Column('debit_amount', Float),
#     Column('credit_amount', Float),
#     Column('balance_amount', Float),
#     Column('type', String),
#     Column('created_at', TIMESTAMP, default=datetime.now)
# )

# # Table to track user-wise table hashes
# user_table_hashes = Table(
#     'user_table_hashes', metadata,
#     Column('id', Integer, primary_key=True),
#     Column('user_id', Integer, ForeignKey('users.id')),
#     Column('table_name', Text, nullable=False),
#     Column('hash', Text, nullable=False),
#     Column('created_at', DateTime, default=datetime.now)
# )

# # Table for storing metadata related to transaction tables
# user_table_metadata = Table(
#     'user_table_metadata', metadata,
#     Column('id', Integer, primary_key=True),
#     Column('user_id', Integer, ForeignKey('users.id')),
#     Column('table_hash_id', Integer, ForeignKey('user_table_hashes.id')),
#     Column('bank_name', Text),
#     Column('account_number', Text),
#     Column('report_period', Text),
#     Column('opening_balance', Float),
#     Column('opening_balance_type', String),
#     Column('closing_balance', Float),
#     Column('closing_balance_type', String),
#     Column('transaction_period', Text),
#     Column('account_holder_name', Text),
#     Column('metadata_hash', Text, nullable=False),
#     Column('created_at', DateTime, default=datetime.now)
# )


# # Create all tables in DB
# def create_tables():
#     metadata.create_all(engine)

