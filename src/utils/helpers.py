"""
Core utility functions
"""
import logging
from typing import Any, Dict, List
from datetime import datetime
import json

logger = logging.getLogger(__name__)


def format_currency(value: float, currency: str = "USD") -> str:
    """
    Format number as currency
    
    Args:
        value: Numeric value
        currency: Currency code
        
    Returns:
        Formatted currency string
    """
    if currency == "USD":
        return f"${value:,.2f}"
    return f"{value:,.2f} {currency}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """
    Format number as percentage
    
    Args:
        value: Numeric value (0.1 = 10%)
        decimals: Number of decimal places
        
    Returns:
        Formatted percentage string
    """
    return f"{value * 100:.{decimals}f}%"


def calculate_growth_rate(current: float, previous: float) -> float:
    """
    Calculate growth rate between two values
    
    Args:
        current: Current value
        previous: Previous value
        
    Returns:
        Growth rate as decimal (0.1 = 10% growth)
    """
    if previous == 0:
        return 0.0
    return (current - previous) / previous


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if division fails
        
    Returns:
        Result of division or default
    """
    if denominator == 0:
        return default
    return numerator / denominator


def parse_sec_date(date_string: str) -> datetime:
    """
    Parse SEC date format to datetime
    
    Args:
        date_string: Date string from SEC filing
        
    Returns:
        Datetime object
    """
    common_formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%B %d, %Y",
        "%b %d, %Y"
    ]
    
    for fmt in common_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date: {date_string}")
    return None


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split a list into chunks
    
    Args:
        lst: Input list
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def save_json(data: Dict, file_path: str):
    """
    Save data to JSON file
    
    Args:
        data: Data to save
        file_path: Path to save file
    """
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved data to {file_path}")
    except Exception as e:
        logger.error(f"Error saving JSON: {str(e)}")


def load_json(file_path: str) -> Dict:
    """
    Load data from JSON file
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        Loaded data
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON: {str(e)}")
        return {}
