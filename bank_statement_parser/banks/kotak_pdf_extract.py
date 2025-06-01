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

    def extract_metadata(self, bank_name: str, lines_per_page: List[List[str]], transactions: List[str] = None) -> dict:
        """
        Extract metadata such as account number, bank name, report period, and opening balance.

        Args:
            bank_name: Name of the bank (inferred).
            lines_per_page: List of list of words for each line on each page.

        Returns:
            metadata: Dictionary containing metadata.
        """
        self.metadata = {
            'bank_name': bank_name,
            'account_number': None,
            'report_period': None,
            'opening_balance': None,
            'opening_balance_type': None,
            'closing_balance': None,
            'closing_balance_type': None,
            'transaction_period': None,
            'account_holder_name': None,
        }
        name=True
        for page in lines_per_page:
            for line in page:
                if name:
                    self.metadata['account_holder_name'] = line[0].strip()
                    name=False
                
                line_str = " ".join(line).strip()

                # Extract account number
                if not self.metadata['account_number'] and 'account_number' in self.patterns:
                    match = self.patterns['account_number'].search(line_str)
                    if match:
                        self.metadata['account_number'] = match.group(1)
        
                # Extract report period
                if not self.metadata['report_period'] and 'report_period' in self.patterns:
                    match = self.patterns['report_period'].search(line_str)
                    if match:
                        self.metadata['report_period'] = (match.group(1), match.group(2))

                # Extract opening balance
                if not self.metadata['opening_balance'] and 'opening_balance' in self.patterns:
                    match = self.patterns['opening_balance'].search(line_str)
                    if match:
                        amount = float(match.group(1).replace(',', ''))
                        direction_upper = match.group(2).upper()
                        
                        self.metadata['opening_balance'] = amount if direction_upper == "CR" else -amount
                        self.metadata['opening_balance_type'] = direction_upper
                        
                # Extract closing balance
                if not self.metadata['closing_balance'] and 'closing_balance' in self.patterns:
                    match = self.patterns['closing_balance'].search(line_str)
                    if match:
                        amount = float(match.group(1).replace(',', ''))
                        direction_upper = match.group(2).upper()
                        
                        self.metadata['closing_balance'] = amount if direction_upper == "CR" else -amount
                        self.metadata['closing_balance_type'] = direction_upper
                
                
                if all([
                    self.metadata['bank_name'],
                    self.metadata['account_number'],
                    self.metadata['report_period'],
                    self.metadata['opening_balance'],
                    self.metadata['closing_balance'],
                    self.metadata['closing_balance_type'],
                    self.metadata['account_holder_name'],
                ]):
                    break
            else:
                continue
            break
        
        self.metadata['transaction_period'] = (
                self.patterns['date'].search(transactions[0]).group(),
                self.patterns['date'].search(transactions[-1]).group()
            )
        
        
        

        return self.metadata