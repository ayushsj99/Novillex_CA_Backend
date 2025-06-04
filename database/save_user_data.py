from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from sqlalchemy.exc import IntegrityError
from .db import engine, users, user_table_hashes, user_table_metadata
import pandas as pd
from datetime import datetime
import hashlib

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

def hash_dataframe(df: pd.DataFrame) -> str:
    """Create a stable hash of a transaction DataFrame"""
    df_copy = df.sort_index(axis=1).sort_values(by=df.columns.tolist()).reset_index(drop=True)
    df_bytes = df_copy.to_csv(index=False).encode()
    return hashlib.sha256(df_bytes).hexdigest()

def hash_metadata(metadata: dict) -> str:
    """Create a hash of the metadata dictionary"""
    clean_meta = {k: str(v) for k, v in sorted(metadata.items())}
    meta_str = str(clean_meta).encode()
    return hashlib.sha256(meta_str).hexdigest()

def get_user_id(username: str) -> int:
    """Get or create a user by username"""
    result = session.execute(
        select(users.c.id).where(users.c.username == username)
    ).fetchone()

    if result:
        return result[0]

    insert_stmt = users.insert().values(username=username, created_at=datetime.now())
    result = session.execute(insert_stmt)
    session.commit()
    return result.inserted_primary_key[0]

def get_existing_hashes(user_id: int) -> dict:
    """Get previously saved hashes for a user"""
    stmt = select(user_table_hashes.c.table_name, user_table_hashes.c.hash).where(
        user_table_hashes.c.user_id == user_id
    )
    return dict(session.execute(stmt).fetchall())

def save_user_and_transactions(username: str, df: pd.DataFrame, metadata_dict: dict):
    user_id = get_user_id(username)
    print(f"Using user_id: {user_id}")

    # Rename DataFrame columns to match DB schema
    df = df.rename(columns={
        'Date': 'date',
        'Transaction ID/Reference Number': 'transaction_id',
        'Particulars': 'particulars',
        'Debit Amount': 'debit_amount',
        'Credit Amount': 'credit_amount',
        'Balance Amount': 'balance_amount',
        'Type': 'type'
    })

    # Add required fields
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['user_id'] = user_id
    df['created_at'] = datetime.now()

    # Generate hashes
    txn_hash = hash_dataframe(df.drop(columns=['user_id', 'created_at']))
    meta_hash = hash_metadata(metadata_dict)

    # Check for duplicates
    existing_hashes = get_existing_hashes(user_id)
    for table, h in existing_hashes.items():
        if h == txn_hash:
            print(f"🚫 Duplicate transaction data already saved in table '{table}'.")
            return

    # Create new versioned table name
    table_version = len(existing_hashes) + 1
    table_name = f"transactions_user_{user_id}_{table_version}"

    try:
        # Save to Postgres (will fail if table exists)
        df.to_sql(table_name, con=engine, if_exists='fail', index=False)

        # Save hash to user_table_hashes
        result = session.execute(user_table_hashes.insert().values(
            user_id=user_id,
            table_name=table_name,
            hash=txn_hash,
            created_at=datetime.now()
        ))
        session.commit()
        table_hash_id = result.inserted_primary_key[0]

        # Save metadata to user_table_metadata
        session.execute(user_table_metadata.insert().values(
            user_id=user_id,
            table_hash_id=table_hash_id,
            bank_name=metadata_dict.get("bank_name"),
            account_number=metadata_dict.get("account_number"),
            report_period=metadata_dict.get("report_period"),
            opening_balance=metadata_dict.get("opening_balance"),
            opening_balance_type=metadata_dict.get("opening_balance_type"),
            closing_balance=metadata_dict.get("closing_balance"),
            closing_balance_type=metadata_dict.get("closing_balance_type"),
            transaction_period=metadata_dict.get("transaction_period"),
            account_holder_name=metadata_dict.get("account_holder_name"),
            metadata_hash=meta_hash,
            created_at=datetime.now()
        ))
        session.commit()

        print(f"✅ Data saved successfully in '{table_name}' with metadata.")
    except IntegrityError as e:
        print(f"❌ Error saving data: {e}")
        session.rollback()
