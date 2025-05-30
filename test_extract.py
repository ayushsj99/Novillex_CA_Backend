import pdfplumber
import pandas as pd
import re
from collections import defaultdict

def clean_amount(amt):
    if not amt:
        return 0.0
    amt = amt.replace(",", "").replace("DR", "").replace("CR", "").strip()
    try:
        return float(amt)
    except:
        return 0.0

def extract_table_from_pdf(pdf_path, page_num=0, line_tol=3):
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_num]
        words = page.extract_words()

    # Group words by line based on their vertical position
    lines = defaultdict(list)
    for word in words:
        placed = False
        for line_top in lines:
            if abs(line_top - word["top"]) < line_tol:
                lines[line_top].append(word)
                placed = True
                break
        if not placed:
            lines[word["top"]].append(word)

    # Sort lines vertically, and sort words within lines horizontally
    sorted_lines = sorted(lines.items(), key=lambda x: x[0])
    for _, line_words in sorted_lines:
        line_words.sort(key=lambda w: w["x0"])

    # Join words to form full lines of text
    text_lines = [" ".join(word["text"] for word in line_words) for _, line_words in sorted_lines]

    # Regex patterns for parsing
    date_pattern = r"\d{2}-\d{2}-\d{4}"
    amount_pattern = r"([0-9,]+\.\d{2}(?:DR|CR)?)"

    table_rows = []

    for line in text_lines:
        date_match = re.match(date_pattern, line)
        if not date_match:
            continue  # Skip lines without a valid starting date
        
        date = date_match.group()
        rest = line[len(date):].strip()

        amounts = re.findall(amount_pattern, rest)
        if not amounts:
            continue  # No amounts found, skip

        # Extract amounts intelligently
        balance = amounts[-1]
        debit = credit = ""
        if len(amounts) == 3:
            debit, credit, balance = amounts
        elif len(amounts) == 2:
            debit, balance = amounts
        elif len(amounts) == 1:
            balance = amounts[0]

        # Remove amounts from rest to isolate particulars and tran_ref
        for amt in amounts:
            rest = rest.replace(amt, "").strip()

        # Split rest into transaction ref and particulars
        parts = rest.split(maxsplit=1)
        tran_ref = parts[0]
        particulars = parts[1] if len(parts) > 1 else ""

        # Append clean and parsed row
        table_rows.append({
            "Date": date,
            "Tran_Ref": tran_ref,
            "Particulars": particulars,
            "Debit": clean_amount(debit),
            "Credit": clean_amount(credit),
            "Balance": clean_amount(balance)
        })

    df = pd.DataFrame(table_rows)
    return df

# Usage
pdf_path = "bank_statement.pdf"
df = extract_table_from_pdf(pdf_path)
print(df.head(10))

# Save to CSV or JSON if needed
df.to_csv("bank_statement_parsed.csv", index=False)
# df.to_json("bank_statement_parsed.json", orient="records")
