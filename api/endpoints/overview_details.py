import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import datetime, timedelta
from database import crud
from collections import defaultdict
import numpy as np
import math
from fastapi.responses import JSONResponse

router = APIRouter()

def clean_nans(obj):
    """Recursively convert all Timestamp and NaNs in nested objects"""
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    elif isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d')
    elif pd.isna(obj):
        return None
    return obj

@router.get("/overview_data")
def analytics_summary(username: Optional[str] = Query(None)):
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")

    user_id = crud.get_user_id_by_username(username)
    if not user_id:
        raise HTTPException(status_code=404, detail="User not found")

    table_names = crud.get_transaction_table_names(user_id)
    metadata_list = crud.get_metadata_by_user_id(user_id)
    if not table_names:
        return JSONResponse(content={})

    all_rows = []
    for table in table_names:
        data = crud.get_transaction_data_from_table(table)
        all_rows.extend(data)

    if not all_rows:
        return JSONResponse(content={})

    df = pd.DataFrame(all_rows)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

    df['debit_amount'] = df['debit_amount'].fillna(0)
    df['credit_amount'] = df['credit_amount'].fillna(0)

    df = df.sort_values(by='date')
    full_range = pd.date_range(df['date'].min(), df['date'].max())
    no_txn_dates = sorted(set(full_range.date) - set(df['date'].dt.date))

    month_df = df.copy()
    month_df['month'] = month_df['date'].dt.to_period('M')
    active_months = month_df['month'].unique()
    full_month_range = pd.period_range(df['date'].min(), df['date'].max(), freq='M')
    no_txn_months = sorted([str(m) for m in full_month_range if m not in active_months])

    end = pd.to_datetime("2024-09-30")
    start_12 = end - pd.DateOffset(months=12) + timedelta(days=1)
    start_3 = end - pd.DateOffset(months=3) + timedelta(days=1)

    df_last_12 = df[(df['date'] >= start_12) & (df['date'] <= end)]
    df_last_3 = df[(df['date'] >= start_3) & (df['date'] <= end)]

    max_debit_12 = df_last_12['debit_amount'].max()
    max_credit_12 = df_last_12['credit_amount'].max()
    max_debit_3 = df_last_3['debit_amount'].max()
    max_credit_3 = df_last_3['credit_amount'].max()

    cash_deposit_9_to_10L = df[(df['credit_amount'] >= 9_00_000) & (df['credit_amount'] <= 10_00_000)]
    cash_deposit_40_to_50k = df[(df['credit_amount'] >= 40_000) & (df['credit_amount'] <= 50_000)]

    atm_withdrawals = df[df['particulars'].str.contains("ATM withdraw", case=False, na=False)]
    atm_large_withdrawals = atm_withdrawals[atm_withdrawals['debit_amount'] > 2000]

    banks_meta = defaultdict(list)
    for meta in metadata_list:
        banks_meta['bank_name'].append(meta['bank_name'])
        banks_meta['account_number'].append(meta['account_number'])
        banks_meta['transaction_period'].append(meta['transaction_period'])
        banks_meta['account_holder_address'] = meta.get('account_holder_name')

    return JSONResponse(content=clean_nans({
    "no_transaction_months": no_txn_months,
    "no_transaction_dates": [d.strftime('%Y-%m-%d') for d in no_txn_dates],
    "account_holder_address": banks_meta.get("account_holder_address"),

    "last_12_months_max_debit_amount": max_debit_12,
    "last_12_months_max_credit_amount": max_credit_12,
    "last_3_months_max_debit_amount": max_debit_3,
    "last_3_months_max_credit_amount": max_credit_3,

    "cash_deposits_9_to_10_lakhs": cash_deposit_9_to_10L[['date', 'credit_amount']].to_dict(orient='records'),
    "cash_deposits_40k_to_50k": cash_deposit_40_to_50k[['date', 'credit_amount']].to_dict(orient='records'),

    "atm_withdrawals_above_2000": atm_large_withdrawals[['date', 'debit_amount']].to_dict(orient='records'),

    "bank_name": banks_meta['bank_name'],
    "account_number": banks_meta['account_number'],
    "transaction_period": banks_meta['transaction_period']
}))
