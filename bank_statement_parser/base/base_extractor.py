import pandas as pd
from typing import List, Dict, Tuple
from database.save_user_data import save_user_and_transactions

class BaseExtractor:
    def __init__(self):
        self.metadata = {}
        self.unmatched_lines = 0
        self.unmatched_lines_no = []
        self.patterns = {}  # To be defined by child class

    def extract_metadata(self, bank_name: str, lines_per_page: List[List[str]], transactions: List[str]):
        """
        Extract metadata from the PDF content.

        Args:
            bank_name: Name of the bank to extract metadata for.
            lines_per_page: Text from the PDF grouped by lines and pages.

        Returns:
            Dictionary containing metadata like opening balance, closing balance, etc.
        """
        # Placeholder for metadata extraction logic
        # This should be implemented in child classes
        raise NotImplementedError("extract_metadata() must be implemented in child class.")

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

        transactions = self.extract_transactions(lines_per_page)
        if not transactions:
            raise ValueError("No transactions found in the document.")
        
        metadata = self.extract_metadata(bank_name, lines_per_page, transactions)
        if not metadata:
            raise ValueError("Metadata could not be extracted.")

        

        df = self.parse_transactions_to_dataframe(transactions)
        if df.empty:
            raise ValueError("Transaction DataFrame is empty.")
        
        save_user_and_transactions('ayush', df, self.metadata)

        return metadata, df, self.unmatched_lines, self.unmatched_lines_no
