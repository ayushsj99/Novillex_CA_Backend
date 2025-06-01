from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from database import crud

router = APIRouter()

REQUIRED_METADATA_FIELDS = [
    "bank_name",
    "account_number",
    "report_period",
    "opening_balance",
    "opening_balance_type",
    "closing_balance",
    "closing_balance_type",
    "transaction_period",
    "account_holder_name"
]

@router.get("/get-metadata")
def fetch_metadata(
    username: Optional[str] = Query(None),
    table_hash_id: Optional[int] = Query(None)
):
    try:
        if not username:
            raise HTTPException(status_code=400, detail="username is required")

        user_id = crud.get_user_id_by_username(username)
        if not user_id:
            raise HTTPException(status_code=404, detail="User not found")

        if table_hash_id:
            metadata_records = crud.get_metadata_by_user_and_hash(user_id, table_hash_id)
        else:
            metadata_records = crud.get_metadata_by_user_id(user_id)

        if not metadata_records:
            raise HTTPException(status_code=404, detail="Metadata not found")

        # Handle list or single dict based on your CRUD return type
        if isinstance(metadata_records, dict):
            filtered = {k: metadata_records[k] for k in REQUIRED_METADATA_FIELDS if k in metadata_records}
        else:
            filtered = [
                {k: record[k] for k in REQUIRED_METADATA_FIELDS if k in record}
                for record in metadata_records
            ]

        return {"metadata": filtered}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
