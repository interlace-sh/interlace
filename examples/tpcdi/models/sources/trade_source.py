"""
TPC-DI Source Models: Trade and Market Data Extraction

Extracts trade transactions, holdings, watches, and market history data.
These represent the high-volume transactional data sources.
"""

from interlace import model
from pathlib import Path
import pandas as pd


def _get_data_path(filename: str) -> str:
    """Get path to source data file."""
    project_root = Path(__file__).parent.parent.parent
    return str(project_root / "data" / "raw" / filename)


@model(
    name="src_trade",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract trade transaction data from source system",
    tags=["source", "extract", "trade", "fact"]
)
def src_trade():
    """
    Extract trade transaction data from source CSV.

    Trade records represent buy/sell orders executed in the market.
    This is the primary transactional fact table source.

    Returns:
        pandas.DataFrame: Raw trade data
    """
    csv_path = _get_data_path("trade.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'trade_id', 'account_id', 'security_id', 'trade_date', 'trade_time',
            'trade_type', 'quantity', 'price', 'commission', 'fees',
            'settlement_date', 'status', 'is_cash'
        ])


@model(
    name="src_holding",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract current holdings data from source system",
    tags=["source", "extract", "holding", "fact"]
)
def src_holding():
    """
    Extract current holdings data from source CSV.

    Holding records represent the current securities owned
    by each account with their cost basis.

    Returns:
        pandas.DataFrame: Raw holdings data
    """
    csv_path = _get_data_path("holding.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'holding_id', 'account_id', 'security_id', 'quantity',
            'cost_basis', 'current_price', 'as_of_date'
        ])


@model(
    name="src_watch",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract customer watch list data from source system",
    tags=["source", "extract", "watch", "fact"]
)
def src_watch():
    """
    Extract customer watch list data from source CSV.

    Watch records represent securities that customers are
    monitoring for potential future trades.

    Returns:
        pandas.DataFrame: Raw watch list data
    """
    csv_path = _get_data_path("watch.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'watch_id', 'customer_id', 'security_id', 'added_date',
            'removed_date', 'is_active'
        ])


@model(
    name="src_market_history",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract daily market price history from source system",
    tags=["source", "extract", "market", "fact"]
)
def src_market_history():
    """
    Extract daily market price history from source CSV.

    Market history records contain daily OHLC (Open/High/Low/Close)
    prices and volume for each security.

    Returns:
        pandas.DataFrame: Raw market history data
    """
    csv_path = _get_data_path("market_history.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'market_id', 'security_id', 'trade_date', 'open_price',
            'high_price', 'low_price', 'close_price', 'volume', 'dividend'
        ])


@model(
    name="src_cash_balance",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract daily cash balance snapshots from source system",
    tags=["source", "extract", "cash", "fact"]
)
def src_cash_balance():
    """
    Extract daily cash balance snapshots from source CSV.

    Cash balance records track the cash position of each account
    over time, including pending transactions.

    Returns:
        pandas.DataFrame: Raw cash balance data
    """
    csv_path = _get_data_path("cash_balance.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'balance_id', 'account_id', 'balance_date', 'cash_balance',
            'pending_deposits', 'pending_withdrawals'
        ])
