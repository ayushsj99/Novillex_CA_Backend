from fastapi import APIRouter, HTTPException
from database.crud import get_user_id_by_username, get_transaction_table_names, get_transaction_data_from_table
import pandas as pd
from datetime import datetime

router = APIRouter()

@router.get("/monthly-avg-balance")
def monthly_avg_balance(username: str):
    # Step 1: Get user ID
    user_id = get_user_id_by_username(username)
    if user_id is None:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")

    # Step 2: Get latest transaction table for user
    table_names = get_transaction_table_names(user_id)
    if not table_names:
        raise HTTPException(status_code=404, detail=f"No transaction tables found for user '{username}'")

    latest_table = sorted(table_names)[-1]

    # Step 3: Load transaction data
    transactions = get_transaction_data_from_table(latest_table)
    if not transactions:
        raise HTTPException(status_code=404, detail="No transactions found in the latest table")

    df = pd.DataFrame(transactions)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date', 'balance_amount'])

    # Step 4: Use accurate daily average logic
    df = df[['date', 'balance_amount']].sort_values('date')
    df = df.drop_duplicates(subset='date', keep='last')  # One balance per day (latest if multiple)

    # Create full date range from min to max
    full_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df = df.set_index('date').reindex(full_range).rename_axis('date')

    # Forward fill missing balances
    df['balance_amount'] = df['balance_amount'].ffill()

    # Group by month and calculate mean
    df['month'] = df.index.to_period('M').astype(str)
    result_df = (
        df.groupby('month')['balance_amount']
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={'balance_amount': 'average_balance'})
    )

    return {
        "username": username,
        "monthly_avg_balance": result_df.to_dict(orient="records")
    }
