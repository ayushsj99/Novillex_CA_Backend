from fastapi import APIRouter, Query, HTTPException
from typing import Optional
import pandas as pd
from database import crud
import numpy as np

router = APIRouter()


@router.get("/get-all-transactions")
def get_all_transactions(username: Optional[str] = Query(None)):
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")

    user_id = crud.get_user_id_by_username(username)
    if not user_id:
        raise HTTPException(status_code=404, detail="User not found")

    table_names = crud.get_transaction_table_names(user_id)
    if not table_names:
        return {"transactions": []}

    all_data = []
    for table in table_names:
        rows = crud.get_transaction_data_from_table(table)
        if rows:
            all_data.extend(rows)

    if not all_data:
        return {"transactions": []}

    df = pd.DataFrame(all_data)

    # Ensure 'date' is in datetime and sort
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df = df.sort_values(by='date')

    # Replace NaN/NaT/inf with None so JSON is valid
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    return {"transactions": df.to_dict(orient='records')}
