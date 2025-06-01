# db_connection.py
from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, MetaData, ForeignKey, TIMESTAMP, Text, DateTime
from datetime import datetime
import sqlalchemy as sa

# PostgreSQL connection URL — update these as per your local setup
DATABASE_URL = "postgresql+psycopg2://postgres:datatoheaven@localhost:5432/bank_statement_db"

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# User table
users = Table('users', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('username', String, unique=True, nullable=False),
    Column('created_at', TIMESTAMP, default=datetime.now)
)

# Transactions table (base schema if needed globally)
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

# Table to track user-wise table hashes
user_table_hashes = Table(
    'user_table_hashes', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('table_name', Text, nullable=False),
    Column('hash', Text, nullable=False),
    Column('created_at', DateTime, default=datetime.now)
)

# Create all tables in DB
def create_tables():
    metadata.create_all(engine)
