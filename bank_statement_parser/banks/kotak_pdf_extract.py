import pandas as pd
import re   
from typing import List
from bank_statement_parser.base.base_extractor import BaseExtractor
from bank_statement_parser.utils.regex_loader import load_regex_patterns_from_json

class KotakExtractor(BaseExtractor):
    """
    A class to extract and parse bank statement data from PDF files.
    """
    
    def __init__(self, bank_name: str):
        """
        Initialize the extractor with the bank name for dynamic regex loading.
        
        Args:
            bank_name (str): The name of the bank, e.g., "Bank of India"
        """
        super().__init__()
        self.patterns = load_regex_patterns_from_json(bank_name)
        
    def parse_transactions_to_dataframe(self, raw_lines: List[str]):
        """
        Parse a list of raw transaction lines and return a structured pandas DataFrame.
        
        Args:
            raw_lines (List[str]): List of raw transaction strings
            
        Returns:
            pd.DataFrame: Structured transaction data
        """
        parsed = []

        for i, line in enumerate(raw_lines):
            match = self.patterns["transaction_detail"].match(line.strip())
            if not match:
                self.unmatched_lines.append(i + 1)  # Line numbers start at 1
                self.unmatched_lines_no.append(line.strip())
                continue
        
    
            data = match.groupdict()

            amount = float(data['amt1'].replace(',', ''))
            balance = float(data['amt2'].replace(',', ''))

            debit_amount = amount if data['amt1_type'] == 'Dr' else None
            credit_amount = amount if data['amt1_type'] == 'Cr' else None

            parsed.append({
            'date': data['date'],
            'transaction_id/reference_number': data['ref'].strip(),
            'particulars': data['part'].strip(),
            'debit_amount': debit_amount,
            'credit_amount': credit_amount,
            'balance_amount': balance,
            'type': data['amt2_type']
            })

        df = pd.DataFrame(parsed)

        df = df.rename(columns={
        'date': 'Date',
        'transaction_id/reference_number': 'Transaction ID/Reference Number',
        'particulars': 'Particulars',
        'debit_amount': 'Debit Amount',
        'credit_amount': 'Credit Amount',
        'balance_amount': 'Balance Amount',
        'type': 'Type'
        })

        df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y', errors='coerce')
        for col in ['Debit Amount', 'Credit Amount', 'Balance Amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    