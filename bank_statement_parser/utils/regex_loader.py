import json
import re
import os
from typing import Dict

def load_regex_patterns_from_json(bank_name: str) -> Dict[str, re.Pattern]:
    """
    Load regex patterns from a JSON file for a given bank.

    Args:
        bank_name (str): Display name of the bank (e.g., 'Bank of India')

    Returns:
        Dict[str, re.Pattern]: Dictionary of compiled regex patterns.
    """
    # Current file's directory = utils/
    base_path = os.path.dirname(__file__)

    # Convert bank name to lowercase underscore format
    filename = bank_name.lower().replace(" ", "_") + "_regex_patterns.json"
    filepath = os.path.join(base_path, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Regex pattern file not found for '{bank_name}' at: {filepath}")

    with open(filepath, "r") as file:
        raw_patterns = json.load(file)

    compiled_patterns = {key: re.compile(pattern) for key, pattern in raw_patterns.items()}
    return compiled_patterns
