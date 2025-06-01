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

        for page in lines_per_page:
            for line in page:
                line_str = " ".join(line).strip()

                # Extract account number
                if not self.metadata['account_number'] and 'account_number' in self.patterns:
                    match = self.patterns['account_number'].search(line_str)
                    if match:
                        self.metadata['account_number'] = match.group(1)

                # Account Holder Name
                acc_holder_match = self.patterns['account_holder_name'].search(line_str)
                if acc_holder_match:
                        self.metadata['account_holder_name'] = acc_holder_match.group(1).strip()
        
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

                if all([
                    self.metadata['bank_name'],
                    self.metadata['account_number'],
                    self.metadata['report_period'],
                    self.metadata['opening_balance']
                ]):
                    break
            else:
                continue
            break
        
        self.metadata['transaction_period'] = (
                self.patterns['date'].search(transactions[0]).group(),
                self.patterns['date'].search(transactions[-1]).group()
            )
        
        match = self.patterns['closing_balance'].search(transactions[-1])
        if match:
        # If closing balance not found in the expected format, use the last transaction 
            closing_amount = match.group(1).replace(',', '')
            self.metadata['closing_balance'] = float(closing_amount)
            self.metadata['closing_balance_type'] = match.group(2).upper()
        # Normalize closing balance
        if self.metadata['closing_balance_type'] == "DR":
            self.metadata['closing_balance'] *= -1
        
        

        return self.metadata