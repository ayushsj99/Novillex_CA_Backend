import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import numpy as np
from database import crud

router = APIRouter()

@router.get("/get-daily-balance")
def get_daily_balance(username: Optional[str] = Query(None)):
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")

    user_id = crud.get_user_id_by_username(username)
    if not user_id:
        raise HTTPException(status_code=404, detail="User not found")

    table_names = crud.get_transaction_table_names(user_id)
    if not table_names:
        return {}

    all_data = []
    for table in table_names:
        rows = crud.get_transaction_data_from_table(table)
        if rows:
            all_data.extend(rows)

    if not all_data:
        return {}

    # Step 1: Create DataFrame
    df = pd.DataFrame(all_data)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date', 'balance_amount'])

    # Step 2: Keep only the last balance per day
    df = df.sort_values('date')
    df = df.drop_duplicates(subset='date', keep='last')

    # Step 3: Create a complete date range and reindex
    full_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df = df.set_index('date').reindex(full_range).rename_axis('date')

    # Step 4: Forward fill missing balances
    df['balance'] = df['balance_amount'].ffill()

    # Step 5: Prepare clean output
    df['month'] = df.index.to_period('M').astype(str)
    df['date'] = df.index.strftime('%Y-%m-%d')
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    result = {
        month: group[['date', 'balance']].to_dict(orient='records')
        for month, group in df.groupby('month')
    }

    return result
