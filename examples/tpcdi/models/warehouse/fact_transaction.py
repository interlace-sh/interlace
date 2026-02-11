"""
TPC-DI Warehouse Models: Fact Tables

Loads transformed fact data into data warehouse fact tables.
These tables form the center of the star schema and reference
dimension tables via foreign keys.

Fact tables use different strategies based on their nature:
- append: For transactional facts (trades, events)
- replace: For snapshot facts (holdings, balances)
"""

from interlace import model
import ibis


@model(
    name="fact_trade",
    schema="warehouse",
    materialise="table",
    strategy="append",
    description="Trade transaction fact table",
    tags=["warehouse", "fact", "trade"]
)
def fact_trade(stg_trade: ibis.Table) -> ibis.Table:
    """
    Load trade fact table.

    Trade facts represent buy/sell transactions and are
    append-only (new trades are added, never updated).

    The staging layer has already enriched the data with:
    - trade_value (quantity * price)
    - total_cost (trade_value + commission + fees)

    Args:
        stg_trade: Staged trade data with calculated fields

    Returns:
        ibis.Table: Trade fact table
    """
    return stg_trade.select([
        'trade_id', 'account_id', 'security_id', 'trade_date', 'trade_time',
        'trade_type', 'quantity', 'price', 'trade_value', 'commission',
        'fees', 'total_cost', 'settlement_date', 'status', 'is_cash'
    ])


@model(
    name="fact_holding",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="Current holdings fact table (snapshot)",
    tags=["warehouse", "fact", "holding"]
)
def fact_holding(stg_holding: ibis.Table) -> ibis.Table:
    """
    Load holdings fact table.

    Holdings represent the current portfolio state and use
    replace strategy (full refresh each run).

    The staging layer has already enriched the data with:
    - market_value (quantity * current_price)
    - total_cost_basis
    - unrealized_gain_loss
    - gain_loss_pct

    Args:
        stg_holding: Staged holdings data with calculated fields

    Returns:
        ibis.Table: Holdings fact table
    """
    return stg_holding.select([
        'holding_id', 'account_id', 'security_id', 'quantity',
        'cost_basis', 'current_price', 'market_value', 'total_cost_basis',
        'unrealized_gain_loss', 'gain_loss_pct', 'as_of_date'
    ])


@model(
    name="fact_watch",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="Customer watch list fact table",
    tags=["warehouse", "fact", "watch"]
)
def fact_watch(stg_watch: ibis.Table) -> ibis.Table:
    """
    Load watch list fact table.

    Watch facts track which securities customers are monitoring.
    Uses replace strategy for full state snapshot.

    Args:
        stg_watch: Staged watch data

    Returns:
        ibis.Table: Watch list fact table
    """
    return stg_watch.select([
        'watch_id', 'customer_id', 'security_id', 'added_date',
        'removed_date', 'is_active'
    ])


@model(
    name="fact_market_history",
    schema="warehouse",
    materialise="table",
    strategy="append",
    description="Daily market price history fact table",
    tags=["warehouse", "fact", "market"]
)
def fact_market_history(stg_market_history: ibis.Table) -> ibis.Table:
    """
    Load market history fact table.

    Market history tracks daily OHLC prices and volume.
    Uses append strategy as historical data is additive.

    The staging layer has already enriched the data with:
    - daily_range (high - low)
    - daily_change (close - open)
    - daily_change_pct

    Args:
        stg_market_history: Staged market history data

    Returns:
        ibis.Table: Market history fact table
    """
    return stg_market_history.select([
        'market_id', 'security_id', 'trade_date', 'open_price',
        'high_price', 'low_price', 'close_price', 'daily_range',
        'daily_change', 'daily_change_pct', 'volume', 'dividend'
    ])


@model(
    name="fact_cash_balance",
    schema="warehouse",
    materialise="table",
    strategy="append",
    description="Daily cash balance fact table",
    tags=["warehouse", "fact", "cash"]
)
def fact_cash_balance(stg_cash_balance: ibis.Table) -> ibis.Table:
    """
    Load cash balance fact table.

    Cash balance tracks account cash positions over time.
    Uses append strategy for historical snapshots.

    The staging layer has already enriched the data with:
    - available_balance (cash + pending_deposits - pending_withdrawals)

    Args:
        stg_cash_balance: Staged cash balance data

    Returns:
        ibis.Table: Cash balance fact table
    """
    return stg_cash_balance.select([
        'balance_id', 'account_id', 'balance_date', 'cash_balance',
        'pending_deposits', 'pending_withdrawals', 'available_balance'
    ])
