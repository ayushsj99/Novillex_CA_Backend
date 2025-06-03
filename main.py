# bank_statement_parser/main.py
import re
import pdfplumber
from collections import defaultdict
from bank_statement_parser.banks.BOI_pdf_extract import BOIExtractor
from bank_statement_parser.banks.kotak_pdf_extract import KotakExtractor
from database.db import create_tables
from fastapi import FastAPI
from api.endpoints import metadata
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.include_router(metadata.router, prefix="/api/v1", tags=["Metadata"])


# Allow frontend to connect (important for Axios + ngrok)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Use specific origin in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],

)



# Ensure database tables are created
create_tables()
# Add other banks as needed

BANK_EXTRACTOR_MAP = {
    "BANK OF INDIA": BOIExtractor,
    "KOTAK MAHINDRA BANK": KotakExtractor,  # Example, replace with actual extractor
    # Add more banks as needed
}

# List of known bank names for fuzzy matching
with open('bank_names.txt', 'r', encoding='utf-8') as f:
    bank_names = [line.strip() for line in f if line.strip()]

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
            line = sorted(lines[y])  # Sort left to right
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
    return extractor.process_bank_statement(lines_per_page,bank_name)

if __name__ == "__main__":
    pdf_path = "./sample_statements/kotak.pdf"
    password = "619132316"

    metadata, df, unmatched_count, unmatched_lines = run_extraction(pdf_path, password)

    print("📄 Metadata:\n", metadata)
    print("\n🔍 First few transactions:")
    print(df.head())
    print(df.columns)
