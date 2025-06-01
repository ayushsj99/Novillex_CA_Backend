import pandas as pd
from typing import List, Dict, Tuple
from database.save_user_data import save_user_and_transactions

class BaseExtractor:
    def __init__(self):
        self.metadata = {}
        self.unmatched_lines = 0
        self.unmatched_lines_no = []
        self.patterns = {}  # To be defined by child class

    def extract_metadata(self, bank_name: str, lines_per_page: List[List[str]]) -> Dict:
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
            'opening_balance_type': None
        }

        for page in lines_per_page:
            for line in page:
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

        return self.metadata

    def extract_transactions(self, lines_per_page: List[List[str]]) -> List[str]:
        """
        Extract transaction lines using regex from the full page content.

        Returns:
            List of matched raw transaction strings.
        """
        transactions = []
        for page in lines_per_page:
            for line in page:
                line_str = " ".join(line).strip()
                if self.patterns.get('transaction') and self.patterns['transaction'].match(line_str):
                    transactions.append(line_str)
        return transactions

    def parse_transactions_to_dataframe(self, raw_lines: List[str]) -> pd.DataFrame:
        """
        Abstract method: Needs to be implemented in child class.
        This should convert transaction lines into a structured DataFrame.
        """
        raise NotImplementedError("parse_transactions_to_dataframe() must be implemented in child class.")

    def process_bank_statement(self, lines_per_page: List[List[str]], bank_name: str) -> Tuple[Dict, pd.DataFrame, int, List[int]]:
        """
        Full processing pipeline to extract metadata and transaction dataframe.

        Args:
            lines_per_page: Text from the PDF grouped by lines and pages.
            bank_name: Detected name of the bank.

        Returns:
            Tuple of metadata dict, transaction dataframe, count of unmatched lines, list of unmatched line numbers.
        """
        if not lines_per_page:
            raise ValueError("No text extracted from PDF.")

        metadata = self.extract_metadata(bank_name, lines_per_page)
        if not metadata:
            raise ValueError("Metadata could not be extracted.")

        transactions = self.extract_transactions(lines_per_page)
        if not transactions:
            raise ValueError("No transactions found in the document.")

        df = self.parse_transactions_to_dataframe(transactions)
        if df.empty:
            raise ValueError("Transaction DataFrame is empty.")
        
        save_user_and_transactions('ayush', df)

        return metadata, df, self.unmatched_lines, self.unmatched_lines_no
