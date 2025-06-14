from sqlalchemy import select
from database.db import engine, user_table_metadata, users, user_table_hashes
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy import Table, Column, Integer, String, Float, DateTime, MetaData, Date, Text

def get_metadata_by_user_id(user_id: int):
    with Session(engine) as session:
        stmt = select(user_table_metadata).where(user_table_metadata.c.user_id == user_id)
        result = session.execute(stmt).fetchall()
        return [dict(row._mapping) for row in result]

def get_metadata_by_table_hash_id(table_hash_id: int):
    with Session(engine) as session:
        stmt = select(user_table_metadata).where(user_table_metadata.c.table_hash_id == table_hash_id)
        result = session.execute(stmt).fetchall()
        return [dict(row._mapping) for row in result]

def get_metadata_by_user_and_hash(user_id: int, table_hash_id: int):
    with Session(engine) as session:
        stmt = select(user_table_metadata).where(
            user_table_metadata.c.user_id == user_id,
            user_table_metadata.c.table_hash_id == table_hash_id
        )
        result = session.execute(stmt).fetchall()
        return [dict(row._mapping) for row in result]
    
def get_user_id_by_username(username: str):
    with Session(engine) as session:
        stmt = select(users.c.id).where(users.c.username == username)
        result = session.execute(stmt).fetchone()
        return result[0] if result else None
    
def get_transaction_table_names(user_id: int):
    """Return list of transaction table names for a user."""
    with Session(engine) as session:
        stmt = select(user_table_hashes.c.table_name).where(user_table_hashes.c.user_id == user_id)
        result = session.execute(stmt).fetchall()
        return [row[0] for row in result]

def get_transaction_data_from_table(table_name: str):
    """Fetch all transaction data from a specific table."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM \"{table_name}\""))
        columns = result.keys()
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def get_opening_balance(user_id: int) -> float:
    stmt = select(user_table_metadata.c.opening_balance).where(user_table_metadata.c.user_id == user_id).limit(1)
    with engine.connect() as conn:
        result = conn.execute(stmt).fetchone()
        return float(result[0]) if result else None
    

def create_transaction_table(table_name: str):
    metadata = MetaData()
    table = Table(
        table_name, metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", Integer),
        Column("date", Date),
        Column("transaction_id", String),
        Column("particulars", Text),
        Column("debit_amount", Float),
        Column("credit_amount", Float),
        Column("balance_amount", Float),
        Column("type", String),
        Column("optional_1", String, nullable=True),
        Column("optional_2", String, nullable=True),
        Column("optional_3", String, nullable=True),
        Column("created_at", DateTime),
    )
    metadata.create_all(engine)  # Actually creates the table in DB


