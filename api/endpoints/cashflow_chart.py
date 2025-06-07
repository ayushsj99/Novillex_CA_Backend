from fastapi import APIRouter, HTTPException
from database.crud import get_user_id_by_username, get_transaction_table_names, get_transaction_data_from_table
import pandas as pd

router = APIRouter()

@router.get("/monthly-cashflow")
def monthly_cashflow_summary(username: str):
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
    df = df.dropna(subset=['date'])

    # Step 4: Group by month and calculate net cashflow
    df['month'] = df['date'].dt.to_period('M').astype(str)
    result_df = (
        df.groupby('month')
        .agg(
            total_credit=pd.NamedAgg(column='credit_amount', aggfunc='sum'),
            total_debit=pd.NamedAgg(column='debit_amount', aggfunc='sum')
        )
        .fillna(0)
        .assign(net_cashflow=lambda x: x['total_credit'] - x['total_debit'])
        .round(2)
        .reset_index()
    )

    return {
        "username": username,
        "monthly_cashflow": result_df[['month', 'net_cashflow']].to_dict(orient="records")
    }
