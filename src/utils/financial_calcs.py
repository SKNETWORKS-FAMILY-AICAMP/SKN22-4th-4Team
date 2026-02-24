"""
Financial calculations and ratios
"""
from typing import Optional


def calculate_profit_margin(net_income: float, revenue: float) -> Optional[float]:
    """Calculate profit margin"""
    if revenue == 0:
        return None
    return net_income / revenue


def calculate_roe(net_income: float, shareholders_equity: float) -> Optional[float]:
    """Calculate Return on Equity"""
    if shareholders_equity == 0:
        return None
    return net_income / shareholders_equity


def calculate_roa(net_income: float, total_assets: float) -> Optional[float]:
    """Calculate Return on Assets"""
    if total_assets == 0:
        return None
    return net_income / total_assets


def calculate_current_ratio(current_assets: float, current_liabilities: float) -> Optional[float]:
    """Calculate Current Ratio"""
    if current_liabilities == 0:
        return None
    return current_assets / current_liabilities


def calculate_debt_to_equity(total_debt: float, shareholders_equity: float) -> Optional[float]:
    """Calculate Debt-to-Equity Ratio"""
    if shareholders_equity == 0:
        return None
    return total_debt / shareholders_equity


def calculate_pe_ratio(price: float, eps: float) -> Optional[float]:
    """Calculate Price-to-Earnings Ratio"""
    if eps == 0:
        return None
    return price / eps


def calculate_pb_ratio(price: float, book_value_per_share: float) -> Optional[float]:
    """Calculate Price-to-Book Ratio"""
    if book_value_per_share == 0:
        return None
    return price / book_value_per_share


def calculate_operating_margin(operating_income: float, revenue: float) -> Optional[float]:
    """Calculate Operating Margin"""
    if revenue == 0:
        return None
    return operating_income / revenue


def calculate_asset_turnover(revenue: float, total_assets: float) -> Optional[float]:
    """Calculate Asset Turnover Ratio"""
    if total_assets == 0:
        return None
    return revenue / total_assets


def calculate_eps(net_income: float, shares_outstanding: float) -> Optional[float]:
    """Calculate Earnings Per Share"""
    if shares_outstanding == 0:
        return None
    return net_income / shares_outstanding
