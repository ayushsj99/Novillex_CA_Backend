#analytics.py

import pandas as pd
from datetime import datetime
import re
from database import crud
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from database import crud

router = APIRouter()

CASH_DEPOSIT_PATTERN = re.compile(r'(?i)(cash deposit|cash dep|cash received)')
CASH_WITHDRAWAL_PATTERN = re.compile(r'(?i)(cash withdrawal|atm withdrawal|cash wdl)')
PENALTY_PATTERN = re.compile(r'(?i)(penalty|fine|chargeback)')
BANK_CHARGES_PATTERN = re.compile(r'(?i)(bank charges|service charge|processing fee)')


def generate_monthly_summary(username: str):
    user_id = crud.get_user_id_by_username(username)
    if not user_id:
        return None  
      

    table_names = crud.get_transaction_table_names(user_id)
    all_data = []
    
    if not table_names:
       return None  # No transaction tables found for the user

    for table in table_names:
        rows = crud.get_transaction_data_from_table(table)
        if rows:
            all_data.extend(rows)

    if not all_data:
        return None

    df = pd.DataFrame(all_data)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['month'] = df['date'].dt.to_period('M')

    summaries = []

    for month, group in df.groupby('month'):
        group = group.sort_values(by='date')
        balances = group['balance_amount'].dropna()

        debit_txns = group[group['debit_amount'].notnull() & (group['debit_amount'] > 0)]
        credit_txns = group[group['credit_amount'].notnull() & (group['credit_amount'] > 0)]

        # Salary detection
        salary_candidates = credit_txns.groupby(['date', 'credit_amount']).size().reset_index(name='count')
        salary_matches = salary_candidates[salary_candidates['count'] > 1]

        # EMI detection (same debit amount every month on same date)
        emi_candidates = debit_txns.groupby(['date', 'debit_amount']).size().reset_index(name='count')
        emi_matches = emi_candidates[emi_candidates['count'] > 1]

        summary = {
            "month": str(month),
            "opening_balance": balances.iloc[0] if not balances.empty else None,
            "closing_balance": balances.iloc[-1] if not balances.empty else None,
            "debit_transaction_amount": debit_txns['debit_amount'].sum(),
            "credit_transaction_amount": credit_txns['credit_amount'].sum(),
            "debit_transaction_count": len(debit_txns),
            "credit_transaction_count": len(credit_txns),
            "salary_income_total": salary_matches['credit_amount'].sum(),
            "salary_income_count": len(salary_matches),
            "emi_total": emi_matches['debit_amount'].sum(),
            "emi_count": len(emi_matches),
            "minimum_balance": balances.min() if not balances.empty else None,
            "maximum_balance": balances.max() if not balances.empty else None,
            "average_balance": balances.mean() if not balances.empty else None,
            "cash_deposit_amount": group[group['particulars'].str.contains(CASH_DEPOSIT_PATTERN, na=False)]['credit_amount'].sum(),
            "cash_deposit_count": group[group['particulars'].str.contains(CASH_DEPOSIT_PATTERN, na=False)].shape[0],
            "cash_withdrawal_amount": group[group['particulars'].str.contains(CASH_WITHDRAWAL_PATTERN, na=False)]['debit_amount'].sum(),
            "cash_withdrawal_count": group[group['particulars'].str.contains(CASH_WITHDRAWAL_PATTERN, na=False)].shape[0],
            "penalty_amount": group[group['particulars'].str.contains(PENALTY_PATTERN, na=False)]['debit_amount'].sum(),
            "penalty_count": group[group['particulars'].str.contains(PENALTY_PATTERN, na=False)].shape[0],
            "bank_charges_amount": group[group['particulars'].str.contains(BANK_CHARGES_PATTERN, na=False)]['debit_amount'].sum(),
            "bank_charges_count": group[group['particulars'].str.contains(BANK_CHARGES_PATTERN, na=False)].shape[0],
            "net_debit": group['debit_amount'].sum(),
            "net_credit": group['credit_amount'].sum(),
            "overdrawn_days": (group['balance_amount'] < 0).sum(),
            "min_eod_balance": balances.min() if not balances.empty else None,
            "max_eod_balance": balances.max() if not balances.empty else None,
            "avg_eod_balance": balances.mean() if not balances.empty else None,
            "balance_on_1st": group[group['date'].dt.day == 1]['balance_amount'].mean() if not group[group['date'].dt.day == 1].empty else None,
            "balance_on_30th": group[group['date'].dt.day == 30]['balance_amount'].mean() if not group[group['date'].dt.day == 30].empty else None,
            "daily_balance_change_pct": balances.pct_change().mean() * 100 if not balances.empty else None
        }

        # FOIR score approximation: EMI / salary (if both are detected)
        if summary['salary_income_total'] and summary['emi_total']:
            summary['foir'] = round(summary['emi_total'] / summary['salary_income_total'], 2)
        else:
            summary['foir'] = None

        summaries.append(summary)

    return summaries


@router.get("/get-monthly-summary")
def get_monthly_summary(username: Optional[str] = Query(None)):
    print("🔥 Entered get_monthly_summary")  # Log step

    if not username:
        raise HTTPException(status_code=400, detail="Username required")

    user_id = crud.get_user_id_by_username(username)
    print(f"🧠 User ID: {user_id}")

    if not user_id:
        raise HTTPException(status_code=404, detail="User not found")

    summary = generate_monthly_summary(username)
    print("📊 Summary Calculated")

    return {"summary": summary}


# @router.get("/get-monthly-summary")
# def get_monthly_summary(username: Optional[str] = Query(None)):
#     try:
#         if not username:
#             raise HTTPException(status_code=400, detail="username is required")

#         user_id = crud.get_user_id_by_username(username)
#         if not user_id:
#             raise HTTPException(status_code=404, detail="User not found")

#         summary = get_monthly_summary(username)
#         if not summary:
#             raise HTTPException(status_code=404, detail="No transaction data found for monthly summary")

#         return {"monthly_summary": summary}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))