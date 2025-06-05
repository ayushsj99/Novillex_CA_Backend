#analytics.py

import pandas as pd
from datetime import datetime
import re
from database import crud
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from database import crud
import numpy as np
from database import crud
from bank_statement_parser.utils.regex_loader import load_regex_patterns_from_json

router = APIRouter()
    


def generate_monthly_summary(username: str):
    user_id = crud.get_user_id_by_username(username)
    if not user_id:
        return None  
      
    # Safely extract metadata and bank name
    metadata_list = crud.get_metadata_by_user_id(user_id)
    metadata = metadata_list[0] if metadata_list else {}
    bank_name = metadata.get("bank_name", "")
    
    try:
        patterns = load_regex_patterns_from_json(bank_name)
        print(f"🔍 Loaded patterns for bank: {bank_name}")
    except ValueError:
        raise HTTPException(status_code=400, detail=f"No regex patterns defined for bank: {bank_name}")
        # Fallback to default patterns if bank not supported
        # patterns = {
        #     "penalty_pattern": r'(?i)(?:penalty|fine|chargeback|PENAL|penal)',
        #     "bank_charges_pattern": r'(?i)(?:bank charges|service charge|processing fee)',
        #     "cash_deposit_pattern": r'(?i)(?:cash deposit|cash dep|cash received)',
        #     "cash_withdrawal_pattern": r'(?i)(?:cash withdrawal|atm withdrawal|cash wdl)',
        # }

    PENALTY_PATTERN = patterns["penalty_pattern"]
    BANK_CHARGES_PATTERN = patterns["bank_charges_pattern"]
    CASH_DEPOSIT_PATTERN = patterns["cash_deposit_pattern"]
    CASH_WITHDRAWAL_PATTERN = patterns["cash_withdrawal_pattern"]

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
    df = df.sort_values(by='date')
    
    # Ensure month column exists
    df['month'] = df['date'].dt.to_period('M')

    # Filter only debit transactions
    all_debit_txns = df[df['debit_amount'].notnull() & (df['debit_amount'] > 0)]

    # Group by debit amount and month
    grouped = all_debit_txns.groupby(['debit_amount', 'month']).size().reset_index(name='count')

    # Count in how many distinct months each debit_amount appears
    emi_frequency = grouped.groupby('debit_amount').size().reset_index(name='distinct_months')

    # Total number of months in the data
    total_months = df['month'].nunique()

    # Consider EMI only if the debit_amount appears in >= N months (you can tune this, maybe N = total_months - 1)
    emi_amounts = emi_frequency[emi_frequency['distinct_months'] >= total_months - 1]['debit_amount'].tolist()


    summaries = []
    
    user_opening_balance = crud.get_opening_balance(user_id)
    
    previous_closing_balance = None
    


    for month, group in df.groupby('month'):
        group = group.sort_values(by='date')
        balances = group['balance_amount'].dropna()

        debit_txns = group[group['debit_amount'].notnull() & (group['debit_amount'] > 0)]
        emi_txns = group[
            (group['debit_amount'].isin(emi_amounts)) &
            (group['debit_amount'].notnull()) &
            (group['debit_amount'] > 0)
            ]

        credit_txns = group[group['credit_amount'].notnull() & (group['credit_amount'] > 0)]

        # Salary detection
        salary_candidates = credit_txns.groupby(['date', 'credit_amount']).size().reset_index(name='count')
        salary_matches = salary_candidates[salary_candidates['count'] > 1]

        
        
        # Calculate opening balance:
        if previous_closing_balance is not None:
            opening_balance = previous_closing_balance
        else:
            opening_balance = float(user_opening_balance) if user_opening_balance is not None else (float(balances.iloc[0]) if not balances.empty else None)

        closing_balance = float(balances.iloc[-1]) if not balances.empty else None

        summary = {
        "month": str(month),
        "opening_balance": opening_balance,
        "closing_balance": closing_balance,
        "debit_transaction_amount": float(debit_txns['debit_amount'].sum()),
        "credit_transaction_amount": float(credit_txns['credit_amount'].sum()),
        "debit_transaction_count": int(len(debit_txns)),
        "credit_transaction_count": int(len(credit_txns)),
        "salary_income_total": float(salary_matches['credit_amount'].sum()),
        "salary_income_count": int(len(salary_matches)),
        "emi_total": float(emi_txns['debit_amount'].sum()),
        "emi_count": int(len(emi_txns)),
        "minimum_balance": float(balances.min()) if not balances.empty else None,
        "maximum_balance": float(balances.max()) if not balances.empty else None,
        "average_balance": float(balances.mean()) if not balances.empty else None,
        "cash_deposit_amount": float(group[group['particulars'].str.contains(CASH_DEPOSIT_PATTERN, na=False)]['credit_amount'].sum()),
        "cash_deposit_count": int(group[group['particulars'].str.contains(CASH_DEPOSIT_PATTERN, na=False)].shape[0]),
        "cash_withdrawal_amount": float(group[group['particulars'].str.contains(CASH_WITHDRAWAL_PATTERN, na=False)]['debit_amount'].sum()),
        "cash_withdrawal_count": int(group[group['particulars'].str.contains(CASH_WITHDRAWAL_PATTERN, na=False)].shape[0]),
        "penalty_amount": float(group[group['particulars'].str.contains(PENALTY_PATTERN, na=False)]['debit_amount'].sum()),
        "penalty_count": int(group[group['particulars'].str.contains(PENALTY_PATTERN, na=False)].shape[0]),
        "bank_charges_amount": float(group[group['particulars'].str.contains(BANK_CHARGES_PATTERN, na=False)]['debit_amount'].sum()),
        "bank_charges_count": int(group[group['particulars'].str.contains(BANK_CHARGES_PATTERN, na=False)].shape[0]),
        "net_debit": float(group['debit_amount'].sum()),
        "net_credit": float(group['credit_amount'].sum()),
        "overdrawn_days": int((group['balance_amount'] < 0).sum()),
        "min_eod_balance": float(balances.min()) if not balances.empty else None,
        "max_eod_balance": float(balances.max()) if not balances.empty else None,
        "avg_eod_balance": float(balances.mean()) if not balances.empty else None,
        "balance_on_1st": float(group[group['date'].dt.day == 1]['balance_amount'].mean()) if not group[group['date'].dt.day == 1].empty else None,
        "balance_on_30th": float(group[group['date'].dt.day == 30]['balance_amount'].mean()) if not group[group['date'].dt.day == 30].empty else None,
        "daily_balance_change_pct": float(balances.pct_change().mean() * 100) if not balances.empty else None,
    }

        previous_closing_balance = closing_balance
        
        # FOIR score approximation: EMI / salary (if both are detected)
        if summary['salary_income_total'] and summary['emi_total']:
            summary['foir'] = round(summary['emi_total'] / summary['salary_income_total'], 2)
        else:
            summary['foir'] = None

        def format_value(v):
            if isinstance(v, (np.integer, int)):
                return int(v)
            elif isinstance(v, (np.floating, float)):
                return round(float(v), 2)
            return v

        summaries.append({k: format_value(v) for k, v in summary.items()})

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
