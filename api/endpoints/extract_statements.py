from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from pathlib import Path
from bank_statement_parser.utils.extraction_core_process import run_extraction
from pandas import Timestamp
import pandas as pd
import numpy as np
import math
import json

router = APIRouter()

def serialize_value(val):
    if isinstance(val, Timestamp):
        return val.isoformat()
    elif isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
    return val

def safe_json(obj):
    if isinstance(obj, dict):
        return {k: safe_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [safe_json(v) for v in obj]
    else:
        return serialize_value(obj)

@router.get("/extract-statement")
def extract_bank_statement(username: str = Query(...)):
    pdf_path = Path("./bank_statements") / f"{username}.pdf"
    password_path = Path("./passwords") / f"{username}.txt"

    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail=f"PDF not found for user: {username}")

    password = ""
    if password_path.exists():
        with open(password_path, "r") as f:
            password = f.read().strip()

    try:
        metadata, df, unmatched_count, unmatched_lines = run_extraction(str(pdf_path), password, username)

        metadata = safe_json(metadata)

        df_serialized = df.head(5).replace({np.nan: None}).applymap(serialize_value)
        df_dict = df_serialized.to_dict(orient="records")

        return JSONResponse(status_code=200, content=safe_json({
            "message": "✅ Extraction successful",
            "metadata": metadata,
            "sample_transactions": df_dict,
            "columns": list(df.columns),
            "unmatched_count": unmatched_count,
            "unmatched_lines": unmatched_lines[:5]
        }))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
