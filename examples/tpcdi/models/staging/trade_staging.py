"""
TPC-DI Staging Models: Trade and Market Data Transformations

Transforms raw trade, holding, watch, and market data with:
- Data cleansing (standardization)
- Calculated fields (trade value, market cap, etc.)
- Data validation
"""

from interlace import model
import ibis


@model(
    name="stg_trade",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged trade data with calculated fields",
    tags=["staging", "transform", "trade"]
)
def stg_trade(src_trade: ibis.Table) -> ibis.Table:
    """
    Transform raw trade data for warehouse loading.

    Transformations:
    - Standardize trade_type and status
    - Calculate trade_value (quantity * price)
    - Calculate total_cost (trade_value + commission + fees)
    - Filter out records with null trade_id

    Args:
        src_trade: Raw trade source data

    Returns:
        ibis.Table: Cleansed trade data with calculated fields
    """
    return (
        src_trade
        # Data validation
        .filter(src_trade.trade_id.notnull())
        .filter(src_trade.account_id.notnull())
        .filter(src_trade.security_id.notnull())
        # Data cleansing and enrichment
        .mutate(
            trade_type=src_trade.trade_type.upper(),
            status=src_trade.status.upper(),
            # Calculated fields
            trade_value=(src_trade.quantity * src_trade.price).round(2),
            total_cost=(
                src_trade.quantity * src_trade.price +
                src_trade.commission +
                src_trade.fees
            ).round(2),
        )
        .select([
            'trade_id', 'account_id', 'security_id', 'trade_date', 'trade_time',
            'trade_type', 'quantity', 'price', 'trade_value', 'commission',
            'fees', 'total_cost', 'settlement_date', 'status', 'is_cash'
        ])
    )


@model(
    name="stg_holding",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged holdings data with calculated fields",
    tags=["staging", "transform", "holding"]
)
def stg_holding(src_holding: ibis.Table) -> ibis.Table:
    """
    Transform raw holdings data for warehouse loading.

    Transformations:
    - Calculate market_value (quantity * current_price)
    - Calculate total_cost_basis (quantity * cost_basis)
    - Calculate unrealized_gain_loss
    - Filter out records with null holding_id

    Args:
        src_holding: Raw holdings source data

    Returns:
        ibis.Table: Cleansed holdings data with calculated fields
    """
    return (
        src_holding
        # Data validation
        .filter(src_holding.holding_id.notnull())
        .filter(src_holding.account_id.notnull())
        .filter(src_holding.security_id.notnull())
        # Calculated fields
        .mutate(
            market_value=(src_holding.quantity * src_holding.current_price).round(2),
            total_cost_basis=(src_holding.quantity * src_holding.cost_basis).round(2),
            unrealized_gain_loss=(
                (src_holding.current_price - src_holding.cost_basis) *
                src_holding.quantity
            ).round(2),
            gain_loss_pct=(
                (src_holding.current_price - src_holding.cost_basis) /
                src_holding.cost_basis * 100
            ).round(2),
        )
        .select([
            'holding_id', 'account_id', 'security_id', 'quantity',
            'cost_basis', 'current_price', 'market_value', 'total_cost_basis',
            'unrealized_gain_loss', 'gain_loss_pct', 'as_of_date'
        ])
    )


@model(
    name="stg_watch",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged watch list data",
    tags=["staging", "transform", "watch"]
)
def stg_watch(src_watch: ibis.Table) -> ibis.Table:
    """
    Transform raw watch list data for warehouse loading.

    Transformations:
    - Filter out records with null watch_id
    - Validate required fields

    Args:
        src_watch: Raw watch source data

    Returns:
        ibis.Table: Cleansed watch data
    """
    return (
        src_watch
        # Data validation
        .filter(src_watch.watch_id.notnull())
        .filter(src_watch.customer_id.notnull())
        .filter(src_watch.security_id.notnull())
        .select([
            'watch_id', 'customer_id', 'security_id', 'added_date',
            'removed_date', 'is_active'
        ])
    )


@model(
    name="stg_market_history",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged market history with calculated fields",
    tags=["staging", "transform", "market"]
)
def stg_market_history(src_market_history: ibis.Table) -> ibis.Table:
    """
    Transform raw market history data for warehouse loading.

    Transformations:
    - Calculate daily_range (high - low)
    - Calculate daily_change (close - open)
    - Calculate daily_change_pct
    - Filter out invalid records

    Args:
        src_market_history: Raw market history source data

    Returns:
        ibis.Table: Cleansed market history with calculated fields
    """
    return (
        src_market_history
        # Data validation
        .filter(src_market_history.market_id.notnull())
        .filter(src_market_history.security_id.notnull())
        # Calculated fields
        .mutate(
            daily_range=(
                src_market_history.high_price - src_market_history.low_price
            ).round(2),
            daily_change=(
                src_market_history.close_price - src_market_history.open_price
            ).round(2),
            daily_change_pct=(
                (src_market_history.close_price - src_market_history.open_price) /
                src_market_history.open_price * 100
            ).round(2),
        )
        .select([
            'market_id', 'security_id', 'trade_date', 'open_price',
            'high_price', 'low_price', 'close_price', 'daily_range',
            'daily_change', 'daily_change_pct', 'volume', 'dividend'
        ])
    )


@model(
    name="stg_cash_balance",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged cash balance data with calculated fields",
    tags=["staging", "transform", "cash"]
)
def stg_cash_balance(src_cash_balance: ibis.Table) -> ibis.Table:
    """
    Transform raw cash balance data for warehouse loading.

    Transformations:
    - Calculate available_balance
    - Filter out invalid records

    Args:
        src_cash_balance: Raw cash balance source data

    Returns:
        ibis.Table: Cleansed cash balance with calculated fields
    """
    return (
        src_cash_balance
        # Data validation
        .filter(src_cash_balance.balance_id.notnull())
        .filter(src_cash_balance.account_id.notnull())
        # Calculated field: available balance
        .mutate(
            available_balance=(
                src_cash_balance.cash_balance +
                src_cash_balance.pending_deposits -
                src_cash_balance.pending_withdrawals
            ).round(2),
        )
        .select([
            'balance_id', 'account_id', 'balance_date', 'cash_balance',
            'pending_deposits', 'pending_withdrawals', 'available_balance'
        ])
    )
