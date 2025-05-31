import pdfplumber
import pandas as pd
import re   
from collections import defaultdict
from typing import List, Dict, Tuple
import traceback
import time

class BankStatementExtractor:
    """
    A class to extract and parse bank statement data from PDF files.
    """
    
    def __init__(self):
        # Regex patterns for extracting different components
        self.metadata = {}
        self.unmatched_lines = 0
        self.unmatched_lines_no = []
        self.patterns = {
            'account_number': re.compile(r"Account Number\s*:([0-9]+(?:/[A-Z]+)?)"),
            'report_period': re.compile(r"Report for the Period\s*:(\d{2}-\d{2}-\d{4})TO(\d{2}-\d{2}-\d{4})"),
            'opening_balance': re.compile(r"Account\s+Opening\s+balance\s*:\s*(\d{1,3}(?:,\d{3})*|\d+)\.\d{2}(DR|CR)"),
            'bank_name': re.compile(r"(?i)\b(?:[A-Z&]{2,}\s+)*BANK(?:\s+[A-Z&]{2,})*\b(?:,\s*\w+)?"),
            'transaction': re.compile(r"^\d{2}-\d{2}-\d{4}[A-Z0-9 ]{8,}\s+.*\d{1,3}(?:,\d{3})*\.\d{2}\s*(?:\d{1,3}(?:,\d{3})*\.\d{2}\s*)?(DR|CR)$"),
            'transaction_detail': re.compile(
                    r"(?P<date>\d{2}-\d{2}-\d{4})\s*"                  # Date
                    r"(?P<tran_id>[A-Z0-9]{6,10})\s+"                    # Transaction ID
                    r"(?:(?P<ref>[A-Za-z0-9][A-Za-z0-9/._-]{8}\d)\s)?"                        # Optional Ref Num
                    r"(?P<part>.*?)"                                # Particulars (non-greedy)
                    r"(?P<amt1>\d{1,3}(?:,\d{2,3})*\.\d{2})?"        # First amount (Debit or Credit)
                    r"\s*"                                          
                    r"(?P<amt2>\d{1,3}(?:,\d{2,3})*\.\d{2})"         # Second amount (always Balance)
                    r"(?P<type>(?i:CR|DR))" 
            )
        }
    
    def extract_text_from_pdf(self, pdf_path: str, password:str = "") -> List[List[str]]:
        """
        Extract text from PDF and organize into lines based on y-coordinates.
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            List[List[str]]: List of lines, where each line is a list of words
        """
        all_words = []
        
        with pdfplumber.open(pdf_path, password=password) as pdf:
            for page in pdf.pages:
                words = page.extract_words(
                    x_tolerance=0.1,      # Tolerance for character merging horizontally
                    y_tolerance=2,        # Tolerance for slight vertical drift
                    keep_blank_chars=True # Preserve spaces within words
                )
                all_words.append(words)
        
        lines_per_page = []
        # Group words by their y-coordinate (line)
        for page in all_words:
            
            lines = defaultdict(list)
            
            # Iterate through words and group them by their y-coordinate
            for word in page:
                if isinstance(word, dict) and 'top' in word:
                    y = round(word['top'], 1)
                    lines[y].append((word['x0'], word['text']))
        
            # Sort lines by y-coordinate and words within each line by x-coordinate
            page_segments = []
            for y in sorted(lines):
                line = sorted(lines[y])  # Sort left to right
                page_segments.append([text for _, text in line])
        
            lines_per_page.append(page_segments)
        
        
        return lines_per_page
    
    def extract_metadata(self, lines_per_page: List[List[str]]) -> Dict:
            """
            Extract metadata from the bank statement (account number, bank name, etc.).
            
            Args:
                segments (List[List[str]]): List of text lines
                
            Returns:
                Dict: Dictionary containing extracted metadata
            """
            self.metadata = {
                'bank_name': None,
                'account_number': None,
                'report_period': None,
                'opening_balance': None,
                'opening_balance_type': None
            }
            
            for page in lines_per_page:
                for line in page:
                    line_str = " ".join(line).strip()
                    
                    # Extract bank name
                    if not self.metadata['bank_name']:
                        match = self.patterns['bank_name'].search(line_str)
                        if match:
                            self.metadata['bank_name'] = match.group().strip()
                    
                    # Extract account number
                    if not self.metadata['account_number']:
                        match = self.patterns['account_number'].search(line_str)
                        if match:
                            self.metadata['account_number'] = match.group(1)
                    
                    # Extract report period
                    if not self.metadata['report_period']:
                        match = self.patterns['report_period'].search(line_str)
                        if match:
                            self.metadata['report_period'] = (match.group(1), match.group(2))
                    
                    # Extract opening balance
                    if not self.metadata['opening_balance']:
                        match = self.patterns['opening_balance'].search(line_str)
                        if match:
                            amount = float(match.group(1).replace(',', ''))
                            direction = match.group(2)
                            self.metadata['opening_balance'] = amount if direction == "CR" else -amount
                            self.metadata['opening_balance_type'] = direction
                    
                    if all([
                                self.metadata['bank_name'],
                                self.metadata['account_number'],
                                self.metadata['report_period'],
                                self.metadata['opening_balance']
                            ]):
                        break  # Break inner loop
                else:
                    continue # Continue outer loop if inner loop didn't break
                break # Break outer loop if all self.metadata is found
                
            return self.metadata
        
    def extract_transactions(self, lines_per_page: List[List[str]]) -> List[str]:
        """
        Extract transaction lines from the segments.
        
        Args:
            segments (List[List[str]]): List of text lines
            
        Returns:
            List[str]: List of transaction strings
        """
        transactions = []
        for page in lines_per_page:
            for line in page:
                line_str = " ".join(line).strip()
                if self.patterns['transaction'].match(line_str):
                    transactions.append(line_str)
        return transactions
    
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
                'transaction_id': data['tran_id'],
                'reference_number': data['ref'].strip() if data['ref'] else "",
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
                'Date', 'Transaction ID', 'Reference Number', 'Particulars',
                'Debit Amount', 'Credit Amount', 'Balance Amount', 'Type'
            ])

        df = pd.DataFrame(transactions)

        df = df.rename(columns={
            'date': 'Date',
            'transaction_id': 'Transaction ID',
            'reference_number': 'Reference Number',
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

    def process_bank_statement(self, pdf_path: str, password:str = "") -> Tuple[Dict, pd.DataFrame]:
        """
        Complete pipeline to process a bank statement PDF.
        
        Args:
            pdf_path (str): Path to the PDF file
            debug (bool): Enable debug output to help troubleshoot
            
        Returns:
            Tuple[Dict, pd.DataFrame]: Metadata and transaction DataFrame
        """
        print("Extracting text from PDF...")
        lines_per_page = self.extract_text_from_pdf(pdf_path, password=password)
        if not lines_per_page:
            raise ValueError("No text extracted from the PDF. Please check the file path or content.")
        metadata = self.extract_metadata(lines_per_page)
        if not metadata['bank_name']:
            raise ValueError("MetaData error. Please check the file content.")
        transaction_lines = self.extract_transactions(lines_per_page)
        if not transaction_lines:
            raise ValueError("No transactions found in the PDF. Please check the file content.")
        df = self.parse_transactions_to_dataframe(transaction_lines)
        if df.empty:
            raise ValueError("No valid transactions found in the PDF. Please check the file content.")
        
        return metadata, df, self.unmatched_lines, self.unmatched_lines_no 


# Usage example
def main():
    """
    Example usage of the BankStatementExtractor
    """
    extractor = BankStatementExtractor()
    
    pdf_path = "bank_statement.pdf"  # Replace with your PDF file path
    pdf_password = ""  # If your PDF is password-protected, set the password here
    csv = True  # Set to True if you want to save the DataFrame as CSV
    start_time = time.time()
    
    try:
        metadata, dataframe , no_of_unmatched_lines , unmatched_line_no_s = extractor.process_bank_statement(pdf_path, password=pdf_password) 
        print("\nMetadata:")
        for key, value in metadata.items():
            print(f"{key}: {value}")
        print("\nTransactions DataFrame:")
        print(dataframe.head())
        print(f"Total unmatched lines: {no_of_unmatched_lines}")
        print(f"List of unmatched lines: {unmatched_line_no_s}")
        if csv:
            output_file = "extracted_transactions_BOI.csv"
            dataframe.to_csv(output_file, index=False)
            print(f"\nTransactions saved to {output_file}")    
        print("\nProcessing complete!")
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()   
        
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds") 


if __name__ == "__main__":
    main()