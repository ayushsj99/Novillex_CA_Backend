# bank_statement_parser/main.py

from database.db import create_tables
# Create tables when app starts
create_tables()
import re
import pdfplumber
from collections import defaultdict
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from bank_statement_parser.banks.BOI_pdf_extract import BOIExtractor
from bank_statement_parser.banks.kotak_pdf_extract import KotakExtractor
from api.endpoints import metadata, month_wise_analysis, transactions, daily_balance_per_month, overview_details, monthly_balance_chart, monthly_debit_credit_chart, cashflow_chart, upload_statement

app = FastAPI()

#####################################################################
# from api.endpoints.supabase_test import fetch_table

# @app.get("/api/v1/test_supabase")
# def test_supabase():
#     try:
#         data = fetch_table("your_table_name")  # Replace with actual table
#         return {"success": True, "rows": data}
#     except Exception as e:
#         return {"success": False, "error": str(e)}

######################################################################


from sqlalchemy import create_engine
# from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# # Fetch variables
# USER = os.getenv("user")
# PASSWORD = os.getenv("password")
# HOST = os.getenv("host")
# PORT = os.getenv("port")
# DBNAME = os.getenv("dbname")

# # Construct the SQLAlchemy connection string
# DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

# # Create the SQLAlchemy engine
# engine = create_engine(DATABASE_URL)
# # If using Transaction Pooler or Session Pooler, we want to ensure we disable SQLAlchemy client side pooling -
# # https://docs.sqlalchemy.org/en/20/core/pooling.html#switching-pool-implementations
# # engine = create_engine(DATABASE_URL, poolclass=NullPool)

# # Test the connection
# try:
#     with engine.connect() as connection:
#         print("Connection successful!")
# except Exception as e:
#     print(f"Failed to connect: {e}")

# Register API routes
app.include_router(metadata.router, prefix="/metadata", tags=["Metadata"])
app.include_router(month_wise_analysis.router, prefix="/summary", tags=["Month Wise Analysis"])
app.include_router(transactions.router, prefix="/transactions", tags=["Transactions"])
app.include_router(daily_balance_per_month.router, prefix="/daily-balance", tags=["Daily Balance"])
app.include_router(overview_details.router, prefix="/overview", tags=["Overview Details"])
app.include_router(monthly_balance_chart.router, prefix="/monthly-balance-chart", tags=["Monthly Balance Chart"])
app.include_router(monthly_debit_credit_chart.router, prefix="/monthly-debit-credit", tags=["Monthly Debit/Credit Summary"])
app.include_router(cashflow_chart.router, prefix="/monthly-cashflow", tags=["Monthly Cashflow Summary"])
app.include_router(upload_statement.router, prefix="/upload", tags=["Upload Statement"])


# CORS middleware (adjust allowed origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Bank extractor registry
BANK_EXTRACTOR_MAP = {
    "BANK OF INDIA": BOIExtractor,
    "KOTAK MAHINDRA BANK": KotakExtractor,
    # Add more bank extractors as needed
}

# Load known bank names for detection
with open('bank_names.txt', 'r', encoding='utf-8') as f:
    bank_names = [line.strip() for line in f if line.strip()]

# Regex pattern to extract bank name-like text
bank_name_pattern = re.compile(
    r"(?i)\b(?:[A-Z&]{2,}\s+)*BANK(?:\s+[A-Z&]{2,})*\b(?:,\s*\w+)?"
)

def extract_lines_from_pdf(pdf_path: str, password: str = ""):
    all_words = []
    with pdfplumber.open(pdf_path, password=password) as pdf:
        for page in pdf.pages:
            words = page.extract_words(
                x_tolerance=0.1,
                y_tolerance=2,
                keep_blank_chars=True
            )
            all_words.append(words)

    lines_per_page = []
    for page in all_words:
        lines = defaultdict(list)
        for word in page:
            if isinstance(word, dict) and 'top' in word:
                y = round(word['top'], 1)
                lines[y].append((word['x0'], word['text']))

        page_segments = []
        for y in sorted(lines):
            line = sorted(lines[y])  # Sort words left to right
            page_segments.append([text for _, text in line])
        lines_per_page.append(page_segments)

    return lines_per_page

def detect_bank_name(lines_per_page):
    for page in lines_per_page:
        for line in page:
            line_str = " ".join(line)
            match = bank_name_pattern.search(line_str)
            if match:
                bank_name_line = match.group().strip()
                matched_banks = [
                    bank for bank in bank_names
                    if re.search(r'\b' + re.escape(bank) + r'\b', bank_name_line, re.IGNORECASE)
                ]
                if matched_banks:
                    return matched_banks[0]
                else:
                    print("⚠️ No matching bank found in:", bank_name_line)
    return None

def run_extraction(pdf_path: str, password: str = ""):
    lines_per_page = extract_lines_from_pdf(pdf_path, password)
    bank_name = detect_bank_name(lines_per_page)

    if not bank_name:
        raise ValueError("Bank name could not be identified from the statement.")
    print(f"✅ Detected Bank: {bank_name}")

    extractor_class = BANK_EXTRACTOR_MAP.get(bank_name.upper())
    if not extractor_class:
        raise ValueError(f"No extractor defined for bank: {bank_name}")

    extractor = extractor_class(bank_name)
    return extractor.process_bank_statement(lines_per_page, bank_name)

# === Test/Development Code ===
# Uncomment below to run standalone for testing

if __name__ == "__main__":
    pdf_path = "./sample_statements/BOI.pdf"
    password = ""

    metadata, df, unmatched_count, unmatched_lines = run_extraction(pdf_path, password)

    print("📄 Metadata:\n", metadata)
    print("\n🔍 First few transactions:")
    print(df.head())
    print(df.columns)

