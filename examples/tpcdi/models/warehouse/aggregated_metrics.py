"""
TPC-DI Warehouse Models: Aggregated Metrics

Creates aggregated metrics and summary tables for reporting.
These provide business intelligence views over the star schema.
"""

from interlace import model
import ibis


@model(
    name="agg_customer_portfolio",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="Customer portfolio aggregation",
    tags=["warehouse", "aggregation", "customer", "portfolio"]
)
def agg_customer_portfolio(
    dim_customer: ibis.Table,
    dim_account: ibis.Table,
    fact_holding: ibis.Table
) -> ibis.Table:
    """
    Aggregate customer portfolio metrics.

    Combines customer, account, and holdings data to show
    total portfolio value per customer.

    Args:
        dim_customer: Customer dimension
        dim_account: Account dimension
        fact_holding: Holdings fact table

    Returns:
        ibis.Table: Customer portfolio summary
    """
    # Join holdings to accounts
    holdings_by_account = fact_holding.group_by('account_id').aggregate(
        total_holdings=fact_holding.holding_id.count(),
        total_market_value=fact_holding.market_value.sum(),
        total_cost_basis=fact_holding.total_cost_basis.sum(),
        total_unrealized_gain=fact_holding.unrealized_gain_loss.sum(),
    )

    # Join to accounts and customers
    account_holdings = dim_account.left_join(
        holdings_by_account,
        dim_account.account_id == holdings_by_account.account_id
    )

    customer_portfolio = account_holdings.group_by('customer_id').aggregate(
        account_count=account_holdings.account_id.count(),
        total_holdings=account_holdings.total_holdings.sum(),
        total_market_value=account_holdings.total_market_value.sum(),
        total_cost_basis=account_holdings.total_cost_basis.sum(),
        total_unrealized_gain=account_holdings.total_unrealized_gain.sum(),
    )

    # Join to customer dimension
    result = customer_portfolio.join(
        dim_customer,
        customer_portfolio.customer_id == dim_customer.customer_id
    )

    return result.select([
        dim_customer.customer_id,
        dim_customer.full_name,
        dim_customer.tier,
        dim_customer.state,
        dim_customer.country,
        customer_portfolio.account_count,
        customer_portfolio.total_holdings,
        customer_portfolio.total_market_value,
        customer_portfolio.total_cost_basis,
        customer_portfolio.total_unrealized_gain,
    ])


@model(
    name="agg_broker_performance",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="Broker performance aggregation",
    tags=["warehouse", "aggregation", "broker", "performance"]
)
def agg_broker_performance(
    dim_broker: ibis.Table,
    dim_account: ibis.Table,
    fact_trade: ibis.Table
) -> ibis.Table:
    """
    Aggregate broker performance metrics.

    Combines broker, account, and trade data to show
    broker activity and revenue.

    Args:
        dim_broker: Broker dimension
        dim_account: Account dimension
        fact_trade: Trade fact table

    Returns:
        ibis.Table: Broker performance summary
    """
    # Join trades to accounts to get broker_id
    trades_with_broker = fact_trade.join(
        dim_account,
        fact_trade.account_id == dim_account.account_id
    )

    # Aggregate by broker
    broker_trades = trades_with_broker.group_by('broker_id').aggregate(
        total_trades=trades_with_broker.trade_id.count(),
        total_trade_value=trades_with_broker.trade_value.sum(),
        total_commission=trades_with_broker.commission.sum(),
    )

    # Count accounts per broker
    broker_accounts = dim_account.group_by('broker_id').aggregate(
        account_count=dim_account.account_id.count(),
    )

    # Join to broker dimension
    result = dim_broker.left_join(
        broker_trades,
        dim_broker.broker_id == broker_trades.broker_id
    ).left_join(
        broker_accounts,
        dim_broker.broker_id == broker_accounts.broker_id
    )

    return result.select([
        dim_broker.broker_id,
        dim_broker.full_name,
        dim_broker.branch,
        dim_broker.commission_rate,
        dim_broker.status,
        broker_accounts.account_count,
        broker_trades.total_trades,
        broker_trades.total_trade_value,
        broker_trades.total_commission,
    ])


@model(
    name="agg_security_performance",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="Security performance aggregation",
    tags=["warehouse", "aggregation", "security", "performance"]
)
def agg_security_performance(
    dim_security: ibis.Table,
    dim_company: ibis.Table,
    fact_market_history: ibis.Table
) -> ibis.Table:
    """
    Aggregate security performance metrics.

    Calculates trading statistics and price metrics per security.

    Args:
        dim_security: Security dimension
        dim_company: Company dimension
        fact_market_history: Market history fact table

    Returns:
        ibis.Table: Security performance summary
    """
    # Aggregate market history by security
    security_stats = fact_market_history.group_by('security_id').aggregate(
        trading_days=fact_market_history.market_id.count(),
        total_volume=fact_market_history.volume.sum(),
        avg_close_price=fact_market_history.close_price.mean(),
        min_close_price=fact_market_history.close_price.min(),
        max_close_price=fact_market_history.close_price.max(),
        avg_daily_range=fact_market_history.daily_range.mean(),
        total_dividends=fact_market_history.dividend.sum(),
    )

    # Join to security dimension
    security_with_stats = dim_security.left_join(
        security_stats,
        dim_security.security_id == security_stats.security_id
    )

    # Join to company dimension
    result = security_with_stats.left_join(
        dim_company,
        security_with_stats.company_id == dim_company.company_id
    )

    return result.select([
        dim_security.security_id,
        dim_security.symbol,
        dim_security.name,
        dim_security.exchange,
        dim_company.name.name('company_name'),
        dim_company.industry,
        security_stats.trading_days,
        security_stats.total_volume,
        security_stats.avg_close_price,
        security_stats.min_close_price,
        security_stats.max_close_price,
        security_stats.avg_daily_range,
        security_stats.total_dividends,
    ])


@model(
    name="agg_daily_trading_summary",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="Daily trading activity summary",
    tags=["warehouse", "aggregation", "daily", "trading"]
)
def agg_daily_trading_summary(fact_trade: ibis.Table) -> ibis.Table:
    """
    Aggregate daily trading activity.

    Shows trading volume and value by date for trend analysis.

    Args:
        fact_trade: Trade fact table

    Returns:
        ibis.Table: Daily trading summary
    """
    return fact_trade.group_by('trade_date').aggregate(
        trade_count=fact_trade.trade_id.count(),
        total_quantity=fact_trade.quantity.sum(),
        total_trade_value=fact_trade.trade_value.sum(),
        total_commission=fact_trade.commission.sum(),
        avg_trade_value=fact_trade.trade_value.mean(),
    ).order_by('trade_date')


@model(
    name="agg_state_summary",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="State-level customer summary",
    tags=["warehouse", "aggregation", "state", "geography"]
)
def agg_state_summary(
    dim_customer: ibis.Table,
    dim_account: ibis.Table
) -> ibis.Table:
    """
    Aggregate customer metrics by state.

    Provides geographic distribution of customers and accounts.

    Args:
        dim_customer: Customer dimension
        dim_account: Account dimension

    Returns:
        ibis.Table: State-level summary
    """
    # Count accounts per customer
    customer_accounts = dim_account.group_by('customer_id').aggregate(
        account_count=dim_account.account_id.count(),
    )

    # Join to customers
    customers_with_accounts = dim_customer.left_join(
        customer_accounts,
        dim_customer.customer_id == customer_accounts.customer_id
    )

    # Aggregate by state
    return customers_with_accounts.group_by(['state', 'country']).aggregate(
        customer_count=customers_with_accounts.customer_id.count(),
        total_accounts=customers_with_accounts.account_count.sum(),
        avg_accounts_per_customer=customers_with_accounts.account_count.mean(),
    ).order_by(['country', 'state'])
