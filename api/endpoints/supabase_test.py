# bank_statement_parser/database/supabase_client.py

import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()  # Load .env variables

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_table(table_name: str):
    """Fetch all rows from a Supabase table."""
    response = supabase.table(table_name).select("*").execute()
    return response.data

def insert_record(table_name: str, data: dict):
    """Insert a record into a Supabase table."""
    response = supabase.table(table_name).insert(data).execute()
    return response.data
