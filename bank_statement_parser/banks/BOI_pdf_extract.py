import pandas as pd  
from typing import List
from bank_statement_parser.base.base_extractor import BaseExtractor
from bank_statement_parser.utils.regex_loader import load_regex_patterns_from_json

class BOIExtractor(BaseExtractor):
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
        
    def parse_transactions_to_dataframe(self, raw_lines: List[str]) -> pd.DataFrame:
        """
        Parse a list of raw transaction lines and return a structured pandas DataFrame.
        
        Args:
            raw_lines (List[str]): List of raw transaction strings
            
        Returns:
            pd.DataFrame: Structured transaction data
        """
        transactions = []
        line_no = 0

        # Initialize balance context from metadata
        opening_balance = self.metadata['opening_balance']
        if opening_balance is None:
            raise ValueError("Opening balance not found in metadata. Make sure extract_metadata() was called before this.")
        
        balance_sign = 1 if opening_balance >= 0 else -1
        previous_balance = abs(opening_balance)

        for line in raw_lines:
            line_no += 1
            match = self.patterns['transaction_detail'].search(line)
            if not match:
                self.unmatched_lines_no.append(line_no)
                self.unmatched_lines += 1
                continue

            data = match.groupdict()

            current_balance = float(data['amt2'].replace(",", ""))
            parsed = {
                'date': data['date'],
                #'transaction_id': data['tran_id'],
                'transaction_id/reference_number':  data['tran_id'] +" "+data['ref'].strip() if data['ref'] and data['tran_id'] else data['tran_id'],
                'particulars': data['part'].strip(),
                'debit_amount': None,
                'credit_amount': None,
                'balance_amount': balance_sign * current_balance,
                'type': data['type'].upper()
            }

            if data['amt1']:
                amt1_value = float(data['amt1'].replace(",", ""))
                # Compare previous and current balance to infer debit or credit
                if current_balance > previous_balance:
                    parsed['debit_amount'] = amt1_value
                else:
                    parsed['credit_amount'] = amt1_value

            previous_balance = current_balance
            transactions.append(parsed)

        if not transactions:
            return pd.DataFrame(columns=[
                'Date', 'Transaction ID/Reference Number', 'Particulars',
                'Debit Amount', 'Credit Amount', 'Balance Amount', 'Type'
            ])


        df = pd.DataFrame(transactions)

        df = df.rename(columns={
            'date': 'Date',
            'transaction_id/reference_number': 'Transaction ID/Reference Number',
            # 'reference_number': 'Reference Number',
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

    