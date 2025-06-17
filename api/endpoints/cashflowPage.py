import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import numpy as np
from database import crud

router = APIRouter()

@router.get("/cashflow-page")
def financial_summary(username: Optional[str] = Query(None)):
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")

    user_id = crud.get_user_id_by_username(username)
    if not user_id:
        raise HTTPException(status_code=404, detail="User not found")

    table_names = crud.get_transaction_table_names(user_id)
    if not table_names:
        raise HTTPException(status_code=404, detail="No transaction tables found")

    all_data = []
    for table in table_names:
        rows = crud.get_transaction_data_from_table(table)
        if rows:
            all_data.extend(rows)

    if not all_data:
        return {}

    df = pd.DataFrame(all_data)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date', 'balance_amount'])
    df = df.sort_values('date')
    df = df.drop_duplicates(subset='date', keep='last')

    # Carry forward daily balance
    full_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df_bal = df.set_index('date').reindex(full_range).rename_axis('date')
    df_bal['balance'] = df_bal['balance_amount'].ffill()
    df_bal['month'] = df_bal.index.to_period('M').astype(str)

    # Main transaction DataFrame
    df_txn = pd.DataFrame(all_data)
    df_txn['date'] = pd.to_datetime(df_txn['date'], errors='coerce')
    df_txn = df_txn.dropna(subset=['date'])

    df_txn['month'] = df_txn['date'].dt.to_period('M').astype(str)

    def compute_summary(df_txn, df_bal, months=None):
        if months:
            last_month = pd.Timestamp.today().to_period('M')
            month_cutoff = last_month - months + 1
            mask = df_txn['month'] >= month_cutoff.strftime('%Y-%m')
            df_txn = df_txn[mask]
            df_bal = df_bal[df_bal['month'] >= month_cutoff.strftime('%Y-%m')]

        summary = (
            df_txn.groupby('month')
            .agg(
                inflow=('credit_amount', 'sum'),
                outflow=('debit_amount', 'sum'),
                inflow_txn_count=('credit_amount', lambda x: x.gt(0).sum()),
                outflow_txn_count=('debit_amount', lambda x: x.gt(0).sum())
            )
            .fillna(0)
        )

        avg_bal = df_bal.groupby('month')['balance'].mean().round(2)

        summary['net_cash_flow'] = summary['inflow'] - summary['outflow']
        summary['monthly_avg_balance'] = avg_bal

        return summary.reset_index().fillna(0)

    # Monthly detailed data
    monthwise = compute_summary(df_txn, df_bal)

    def aggregate_period(df, period_label, months):
        last_months = df.tail(months)
        return {
            "period": period_label,
            "inflow": round(last_months['inflow'].sum(), 2),
            "outflow": round(last_months['outflow'].sum(), 2),
            "net_cash_flow": round(last_months['net_cash_flow'].sum(), 2),
            "monthly_avg_balance": round(last_months['monthly_avg_balance'].mean(), 2),
            "inflow_txn_count": int(last_months['inflow_txn_count'].sum()),
            "outflow_txn_count": int(last_months['outflow_txn_count'].sum()),
        }

    return {
        "username": username,
        "monthwise_summary": monthwise.to_dict(orient='records'),
        "aggregates": [
            aggregate_period(monthwise, "Last 3 Months", 3),
            aggregate_period(monthwise, "Last 6 Months", 6),
            aggregate_period(monthwise, "Last 9 Months", 9),
            aggregate_period(monthwise, "Last 12 Months", 12),
        ]
    }
