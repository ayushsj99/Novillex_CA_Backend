# updated_save.py
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from .db import engine, users, user_table_hashes, user_table_metadata
import pandas as pd
from datetime import datetime
import hashlib

Session = sessionmaker(bind=engine)
session = Session()

def hash_dataframe(df: pd.DataFrame) -> str:
    df_copy = df.sort_index(axis=1).sort_values(by=df.columns.tolist()).reset_index(drop=True)
    df_bytes = df_copy.to_csv(index=False).encode()
    return hashlib.sha256(df_bytes).hexdigest()

def get_user_id(username: str):
    result = session.execute(select(users.c.id).where(users.c.username == username)).fetchone()
    if result:
        return result[0]
    insert_user = users.insert().values(username=username, created_at=datetime.now())
    result = session.execute(insert_user)
    session.commit()
    return result.inserted_primary_key[0]

def get_existing_hashes(user_id):
    stmt = select(user_table_hashes.c.table_name, user_table_hashes.c.hash).where(user_table_hashes.c.user_id == user_id)
    return dict(session.execute(stmt).fetchall())

def hash_metadata(metadata: dict) -> str:
    """Generate hash for metadata dict"""
    clean_meta = {k: str(v) for k, v in sorted(metadata.items())}
    meta_str = str(clean_meta).encode()
    return hashlib.sha256(meta_str).hexdigest()

def save_user_and_transactions(username: str, df: pd.DataFrame, metadata_dict: dict):
    user_id = get_user_id(username)
    print(f"Using user_id: {user_id}")

    # Prepare transaction DataFrame
    df = df.rename(columns={
        'Date': 'date',
        'Transaction ID/Reference Number': 'transaction_id',
        'Particulars': 'particulars',
        'Debit Amount': 'debit_amount',
        'Credit Amount': 'credit_amount',
        'Balance Amount': 'balance_amount',
        'Type': 'type'
    })
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['user_id'] = user_id
    df['created_at'] = datetime.now()

    new_txn_hash = hash_dataframe(df.drop(columns=['user_id', 'created_at']))
    new_meta_hash = hash_metadata(metadata_dict)

    existing_hashes = get_existing_hashes(user_id)
    for table, h in existing_hashes.items():
        if h == new_txn_hash:
            print(f"🚫 Transactions already saved in '{table}'.")
            return

    # Save transactions to a new table
    table_name = f"transactions_user_{user_id}_{len(existing_hashes) + 1}"
    df.to_sql(table_name, con=engine, if_exists='fail', index=False)

    # Save to user_table_hashes
    result = session.execute(user_table_hashes.insert().values(
        user_id=user_id,
        table_name=table_name,
        hash=new_txn_hash,
        created_at=datetime.now()
    ))
    session.commit()
    table_hash_id = result.inserted_primary_key[0]

    # Save metadata
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
        metadata_hash=new_meta_hash,
        created_at=datetime.now()
    ))
    session.commit()

    print(f"✅ Transactions saved in '{table_name}', metadata stored.")
