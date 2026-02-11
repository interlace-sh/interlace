"""
TPC-DI Data Generator

Generates synthetic data following the TPC-DI specification for a financial
brokerage data warehouse. This creates data for:

- Customer management (customers, accounts)
- Broker information
- Company and security master data
- Trade transactions
- Market history
- Customer holdings and watches

Scale factors:
- SF=1: ~1000 customers, ~3000 accounts, ~10000 trades (default)
- SF=10: ~10000 customers, ~30000 accounts, ~100000 trades
- SF=100: ~100000 customers, ~300000 accounts, ~1000000 trades

Usage:
    python generate_data.py [--scale-factor 1]
"""

import random
import string
from datetime import datetime, timedelta
from pathlib import Path
import argparse

# Try pandas, fall back to CSV writer if not available
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    import csv
    HAS_PANDAS = False


# Constants
STATES = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI',
          'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI']

COUNTRIES = ['USA', 'USA', 'USA', 'USA', 'Canada', 'Mexico']  # Weighted towards USA

ACCOUNT_TYPES = ['Individual', 'Joint', 'IRA', 'Roth IRA', '401k', 'Trust', 'Custodial']

ACCOUNT_STATUSES = ['ACTIVE', 'ACTIVE', 'ACTIVE', 'ACTIVE', 'INACTIVE', 'CLOSED']  # Weighted

INDUSTRIES = ['Technology', 'Healthcare', 'Finance', 'Energy', 'Consumer',
              'Industrial', 'Materials', 'Utilities', 'Real Estate', 'Communication']

EXCHANGES = ['NYSE', 'NASDAQ', 'AMEX']

TRADE_TYPES = ['Buy', 'Sell', 'Buy to Cover', 'Sell Short']

TRADE_STATUSES = ['Completed', 'Completed', 'Completed', 'Pending', 'Cancelled']  # Weighted

FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael',
               'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan',
               'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher',
               'Lisa', 'Daniel', 'Nancy', 'Matthew', 'Betty', 'Anthony', 'Margaret',
               'Mark', 'Sandra', 'Donald', 'Ashley', 'Steven', 'Kimberly', 'Paul', 'Emily']

LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
              'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez',
              'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
              'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez']

COMPANY_SUFFIXES = ['Inc.', 'Corp.', 'Ltd.', 'LLC', 'Holdings', 'Group', 'Technologies',
                    'Systems', 'Solutions', 'Services', 'Partners', 'Enterprises']


def random_date(start_date: datetime, end_date: datetime) -> datetime:
    """Generate random date between start and end."""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)


def random_phone() -> str:
    """Generate random phone number."""
    return f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"


def random_email(first_name: str, last_name: str) -> str:
    """Generate random email address."""
    domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'email.com', 'mail.com']
    suffix = random.randint(1, 999) if random.random() > 0.5 else ''
    return f"{first_name.lower()}.{last_name.lower()}{suffix}@{random.choice(domains)}"


def random_address() -> str:
    """Generate random street address."""
    number = random.randint(1, 9999)
    streets = ['Main St', 'Oak Ave', 'Maple Dr', 'Cedar Ln', 'Pine Rd',
               'Elm St', 'Park Ave', 'Lake Dr', 'Hill Rd', 'Valley Way']
    return f"{number} {random.choice(streets)}"


def random_city() -> str:
    """Generate random city name."""
    prefixes = ['Spring', 'Lake', 'River', 'Oak', 'Cedar', 'Pine', 'Hill', 'Valley', 'Green', 'Fair']
    suffixes = ['field', 'ville', 'town', 'city', 'wood', 'dale', 'view', 'port', 'land', 'mont']
    return f"{random.choice(prefixes)}{random.choice(suffixes)}"


def random_zip() -> str:
    """Generate random ZIP code."""
    return f"{random.randint(10000, 99999)}"


def random_ticker(company_name: str) -> str:
    """Generate ticker symbol from company name."""
    words = company_name.split()
    if len(words) >= 2:
        ticker = words[0][0] + words[1][0]
    else:
        ticker = words[0][:3]
    # Add random suffix to ensure uniqueness
    ticker += ''.join(random.choices(string.ascii_uppercase, k=random.randint(1, 2)))
    return ticker.upper()[:5]


class TPCDIDataGenerator:
    """Generates TPC-DI benchmark data."""

    def __init__(self, scale_factor: int = 1, seed: int = 42):
        """
        Initialize generator.

        Args:
            scale_factor: Scale factor (1 = ~1000 customers)
            seed: Random seed for reproducibility
        """
        self.sf = scale_factor
        random.seed(seed)

        # Base counts scaled by SF
        self.num_customers = 1000 * scale_factor
        self.num_brokers = 50 * scale_factor
        self.num_companies = 200 * scale_factor
        self.num_securities = 500 * scale_factor

        # Derived counts
        self.num_accounts = self.num_customers * 3  # ~3 accounts per customer
        self.num_trades = self.num_accounts * 10  # ~10 trades per account
        self.num_holdings = self.num_accounts * 5  # ~5 holdings per account
        self.num_watches = self.num_customers * 3  # ~3 watches per customer

        # Date range
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2024, 12, 31)

        # Generated data references (for foreign keys)
        self.customer_ids = []
        self.broker_ids = []
        self.company_ids = []
        self.security_ids = []
        self.account_ids = []

    def _save_data(self, data: list, columns: list, filename: str, output_dir: Path):
        """Save data to CSV file."""
        filepath = output_dir / filename

        if HAS_PANDAS:
            df = pd.DataFrame(data, columns=columns)
            df.to_csv(filepath, index=False)
        else:
            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(data)

        print(f"  Generated {filepath.name}: {len(data)} rows")

    def generate_date_dimension(self, output_dir: Path):
        """Generate DimDate reference data."""
        data = []
        columns = ['date_id', 'date_value', 'year', 'quarter', 'month', 'month_name',
                   'day', 'day_of_week', 'day_name', 'week_of_year', 'is_weekend',
                   'is_holiday', 'fiscal_year', 'fiscal_quarter']

        current = self.start_date
        date_id = 1
        while current <= self.end_date:
            is_weekend = current.weekday() >= 5
            data.append([
                date_id,
                current.strftime('%Y-%m-%d'),
                current.year,
                (current.month - 1) // 3 + 1,
                current.month,
                current.strftime('%B'),
                current.day,
                current.weekday() + 1,
                current.strftime('%A'),
                current.isocalendar()[1],
                1 if is_weekend else 0,
                0,  # Holiday flag (simplified)
                current.year,
                (current.month - 1) // 3 + 1
            ])
            current += timedelta(days=1)
            date_id += 1

        self._save_data(data, columns, 'dim_date.csv', output_dir)

    def generate_time_dimension(self, output_dir: Path):
        """Generate DimTime reference data."""
        data = []
        columns = ['time_id', 'time_value', 'hour', 'minute', 'second',
                   'am_pm', 'market_hours']

        for hour in range(24):
            for minute in range(60):
                time_id = hour * 60 + minute + 1
                time_str = f"{hour:02d}:{minute:02d}:00"
                am_pm = 'AM' if hour < 12 else 'PM'
                # Market hours: 9:30 AM - 4:00 PM
                market_hours = 1 if (9 <= hour < 16) or (hour == 9 and minute >= 30) else 0

                data.append([
                    time_id, time_str, hour, minute, 0, am_pm, market_hours
                ])

        self._save_data(data, columns, 'dim_time.csv', output_dir)

    def generate_brokers(self, output_dir: Path):
        """Generate broker data."""
        data = []
        columns = ['broker_id', 'first_name', 'last_name', 'email', 'phone',
                   'branch', 'commission_rate', 'hire_date', 'status']

        branches = ['New York', 'Chicago', 'Los Angeles', 'Houston', 'Phoenix',
                    'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Austin']

        for i in range(1, self.num_brokers + 1):
            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            data.append([
                i,
                first_name,
                last_name,
                f"{first_name.lower()}.{last_name.lower()}@broker.com",
                random_phone(),
                random.choice(branches),
                round(random.uniform(0.01, 0.05), 4),
                random_date(datetime(2010, 1, 1), self.start_date).strftime('%Y-%m-%d'),
                random.choice(['ACTIVE', 'ACTIVE', 'ACTIVE', 'INACTIVE'])
            ])
            self.broker_ids.append(i)

        self._save_data(data, columns, 'broker.csv', output_dir)

    def generate_companies(self, output_dir: Path):
        """Generate company master data."""
        data = []
        columns = ['company_id', 'name', 'industry', 'ceo', 'description',
                   'founded_date', 'address', 'city', 'state', 'country',
                   'employees', 'status']

        company_prefixes = ['Global', 'United', 'American', 'National', 'Premier',
                           'Pacific', 'Atlantic', 'Central', 'Advanced', 'Dynamic']
        company_cores = ['Tech', 'Health', 'Finance', 'Energy', 'Media',
                        'Auto', 'Aero', 'Bio', 'Data', 'Net']

        for i in range(1, self.num_companies + 1):
            name = f"{random.choice(company_prefixes)} {random.choice(company_cores)} {random.choice(COMPANY_SUFFIXES)}"
            ceo_first = random.choice(FIRST_NAMES)
            ceo_last = random.choice(LAST_NAMES)

            data.append([
                i,
                name,
                random.choice(INDUSTRIES),
                f"{ceo_first} {ceo_last}",
                f"Leading provider of {random.choice(INDUSTRIES).lower()} solutions",
                random_date(datetime(1950, 1, 1), datetime(2015, 1, 1)).strftime('%Y-%m-%d'),
                random_address(),
                random_city(),
                random.choice(STATES),
                'USA',
                random.randint(100, 100000),
                'ACTIVE'
            ])
            self.company_ids.append(i)

        self._save_data(data, columns, 'company.csv', output_dir)

    def generate_securities(self, output_dir: Path):
        """Generate security/stock master data."""
        data = []
        columns = ['security_id', 'symbol', 'name', 'company_id', 'exchange',
                   'issue_date', 'shares_outstanding', 'first_trade_date',
                   'dividend', 'status']

        used_symbols = set()

        for i in range(1, self.num_securities + 1):
            company_id = random.choice(self.company_ids) if self.company_ids else i % self.num_companies + 1

            # Generate unique symbol
            while True:
                symbol = ''.join(random.choices(string.ascii_uppercase, k=random.randint(3, 4)))
                if symbol not in used_symbols:
                    used_symbols.add(symbol)
                    break

            issue_date = random_date(datetime(2000, 1, 1), self.start_date)

            data.append([
                i,
                symbol,
                f"{symbol} Common Stock",
                company_id,
                random.choice(EXCHANGES),
                issue_date.strftime('%Y-%m-%d'),
                random.randint(1000000, 1000000000),
                issue_date.strftime('%Y-%m-%d'),
                round(random.uniform(0, 5), 2),
                'ACTIVE'
            ])
            self.security_ids.append(i)

        self._save_data(data, columns, 'security.csv', output_dir)

    def generate_customers(self, output_dir: Path):
        """Generate customer data with SCD Type 2 fields."""
        data = []
        columns = ['customer_id', 'tax_id', 'first_name', 'last_name', 'gender',
                   'tier', 'dob', 'email', 'phone', 'address_line1', 'address_line2',
                   'city', 'state', 'postal_code', 'country', 'national_id',
                   'effective_date', 'end_date', 'is_current']

        for i in range(1, self.num_customers + 1):
            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            gender = 'M' if first_name in ['James', 'John', 'Robert', 'Michael', 'William',
                                           'David', 'Richard', 'Joseph', 'Thomas', 'Charles',
                                           'Christopher', 'Daniel', 'Matthew', 'Anthony', 'Mark',
                                           'Donald', 'Steven', 'Paul'] else 'F'

            effective_date = random_date(self.start_date, self.end_date)

            data.append([
                i,
                f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}",
                first_name,
                last_name,
                gender,
                random.randint(1, 3),  # Tier 1-3
                random_date(datetime(1940, 1, 1), datetime(2005, 1, 1)).strftime('%Y-%m-%d'),
                random_email(first_name, last_name),
                random_phone(),
                random_address(),
                '',
                random_city(),
                random.choice(STATES),
                random_zip(),
                random.choice(COUNTRIES),
                f"NID{i:08d}",
                effective_date.strftime('%Y-%m-%d'),
                '9999-12-31',  # End date for current record
                1  # Is current
            ])
            self.customer_ids.append(i)

        self._save_data(data, columns, 'customer.csv', output_dir)

    def generate_accounts(self, output_dir: Path):
        """Generate account data."""
        data = []
        columns = ['account_id', 'customer_id', 'broker_id', 'account_type',
                   'status', 'open_date', 'close_date', 'tax_status']

        account_id = 1
        for customer_id in self.customer_ids:
            # Each customer has 1-5 accounts
            num_customer_accounts = random.randint(1, 5)
            for _ in range(num_customer_accounts):
                broker_id = random.choice(self.broker_ids) if self.broker_ids else 1
                status = random.choice(ACCOUNT_STATUSES)
                open_date = random_date(self.start_date, self.end_date)
                close_date = ''
                if status == 'CLOSED':
                    close_date = random_date(open_date, self.end_date).strftime('%Y-%m-%d')

                data.append([
                    account_id,
                    customer_id,
                    broker_id,
                    random.choice(ACCOUNT_TYPES),
                    status,
                    open_date.strftime('%Y-%m-%d'),
                    close_date,
                    random.choice(['Taxable', 'Tax-Deferred', 'Tax-Free'])
                ])
                self.account_ids.append(account_id)
                account_id += 1

                if account_id > self.num_accounts:
                    break
            if account_id > self.num_accounts:
                break

        self._save_data(data, columns, 'account.csv', output_dir)

    def generate_trades(self, output_dir: Path):
        """Generate trade transaction data."""
        data = []
        columns = ['trade_id', 'account_id', 'security_id', 'trade_date', 'trade_time',
                   'trade_type', 'quantity', 'price', 'commission', 'fees',
                   'settlement_date', 'status', 'is_cash']

        for i in range(1, self.num_trades + 1):
            account_id = random.choice(self.account_ids) if self.account_ids else i % self.num_accounts + 1
            security_id = random.choice(self.security_ids) if self.security_ids else i % self.num_securities + 1
            trade_date = random_date(self.start_date, self.end_date)

            quantity = random.randint(1, 1000) * 10
            price = round(random.uniform(5, 500), 2)
            commission = round(price * quantity * random.uniform(0.001, 0.01), 2)

            data.append([
                i,
                account_id,
                security_id,
                trade_date.strftime('%Y-%m-%d'),
                f"{random.randint(9, 16):02d}:{random.randint(0, 59):02d}:00",
                random.choice(TRADE_TYPES),
                quantity,
                price,
                commission,
                round(random.uniform(0, 10), 2),
                (trade_date + timedelta(days=2)).strftime('%Y-%m-%d'),
                random.choice(TRADE_STATUSES),
                random.choice([0, 1])
            ])

        self._save_data(data, columns, 'trade.csv', output_dir)

    def generate_holdings(self, output_dir: Path):
        """Generate current holdings data."""
        data = []
        columns = ['holding_id', 'account_id', 'security_id', 'quantity',
                   'cost_basis', 'current_price', 'as_of_date']

        holding_id = 1
        for account_id in self.account_ids[:self.num_holdings]:
            # Each account has 1-10 holdings
            num_holdings = random.randint(1, 10)
            used_securities = set()

            for _ in range(num_holdings):
                security_id = random.choice(self.security_ids) if self.security_ids else holding_id % self.num_securities + 1
                if security_id in used_securities:
                    continue
                used_securities.add(security_id)

                quantity = random.randint(10, 10000)
                cost_basis = round(random.uniform(10, 400), 2)
                current_price = round(cost_basis * random.uniform(0.5, 2.0), 2)

                data.append([
                    holding_id,
                    account_id,
                    security_id,
                    quantity,
                    cost_basis,
                    current_price,
                    self.end_date.strftime('%Y-%m-%d')
                ])
                holding_id += 1

        self._save_data(data, columns, 'holding.csv', output_dir)

    def generate_watches(self, output_dir: Path):
        """Generate customer watch list data."""
        data = []
        columns = ['watch_id', 'customer_id', 'security_id', 'added_date',
                   'removed_date', 'is_active']

        watch_id = 1
        for customer_id in self.customer_ids[:self.num_watches]:
            # Each customer watches 1-10 securities
            num_watches = random.randint(1, 10)
            used_securities = set()

            for _ in range(num_watches):
                security_id = random.choice(self.security_ids) if self.security_ids else watch_id % self.num_securities + 1
                if security_id in used_securities:
                    continue
                used_securities.add(security_id)

                added_date = random_date(self.start_date, self.end_date)
                is_active = random.choice([1, 1, 1, 0])  # 75% active
                removed_date = ''
                if not is_active:
                    removed_date = random_date(added_date, self.end_date).strftime('%Y-%m-%d')

                data.append([
                    watch_id,
                    customer_id,
                    security_id,
                    added_date.strftime('%Y-%m-%d'),
                    removed_date,
                    is_active
                ])
                watch_id += 1

        self._save_data(data, columns, 'watch.csv', output_dir)

    def generate_market_history(self, output_dir: Path):
        """Generate daily market price history."""
        data = []
        columns = ['market_id', 'security_id', 'trade_date', 'open_price',
                   'high_price', 'low_price', 'close_price', 'volume', 'dividend']

        market_id = 1
        # Generate for subset of securities to keep data manageable
        securities_subset = self.security_ids[:min(100, len(self.security_ids))]

        for security_id in securities_subset:
            # Generate price history
            base_price = random.uniform(20, 200)
            current_date = self.start_date

            while current_date <= self.end_date:
                # Skip weekends
                if current_date.weekday() < 5:
                    # Random walk for prices
                    change = random.uniform(-0.03, 0.03)
                    base_price = max(1, base_price * (1 + change))

                    open_price = round(base_price * random.uniform(0.98, 1.02), 2)
                    close_price = round(base_price * random.uniform(0.98, 1.02), 2)
                    high_price = round(max(open_price, close_price) * random.uniform(1.0, 1.02), 2)
                    low_price = round(min(open_price, close_price) * random.uniform(0.98, 1.0), 2)

                    data.append([
                        market_id,
                        security_id,
                        current_date.strftime('%Y-%m-%d'),
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        random.randint(10000, 10000000),
                        0  # Dividend (quarterly)
                    ])
                    market_id += 1

                current_date += timedelta(days=1)

        self._save_data(data, columns, 'market_history.csv', output_dir)

    def generate_cash_balances(self, output_dir: Path):
        """Generate daily cash balance snapshots."""
        data = []
        columns = ['balance_id', 'account_id', 'balance_date', 'cash_balance',
                   'pending_deposits', 'pending_withdrawals']

        balance_id = 1
        # Generate for subset of accounts
        accounts_subset = self.account_ids[:min(500, len(self.account_ids))]

        for account_id in accounts_subset:
            # Generate monthly snapshots
            base_balance = random.uniform(1000, 1000000)
            current_date = self.start_date

            while current_date <= self.end_date:
                # Random walk for balance
                change = random.uniform(-0.05, 0.10)
                base_balance = max(0, base_balance * (1 + change))

                data.append([
                    balance_id,
                    account_id,
                    current_date.strftime('%Y-%m-%d'),
                    round(base_balance, 2),
                    round(random.uniform(0, base_balance * 0.1), 2),
                    round(random.uniform(0, base_balance * 0.05), 2)
                ])
                balance_id += 1
                current_date += timedelta(days=30)  # Monthly

        self._save_data(data, columns, 'cash_balance.csv', output_dir)

    def generate_all(self, output_dir: Path):
        """Generate all TPC-DI data files."""
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Generating TPC-DI data with scale factor {self.sf}")
        print(f"Output directory: {output_dir}")
        print()

        # Generate reference/dimension data first
        print("Generating reference data...")
        self.generate_date_dimension(output_dir)
        self.generate_time_dimension(output_dir)

        # Generate master data (need these IDs for transactional data)
        print("\nGenerating master data...")
        self.generate_brokers(output_dir)
        self.generate_companies(output_dir)
        self.generate_securities(output_dir)
        self.generate_customers(output_dir)
        self.generate_accounts(output_dir)

        # Generate transactional/fact data
        print("\nGenerating transactional data...")
        self.generate_trades(output_dir)
        self.generate_holdings(output_dir)
        self.generate_watches(output_dir)
        self.generate_market_history(output_dir)
        self.generate_cash_balances(output_dir)

        print("\nData generation complete!")
        print(f"\nSummary:")
        print(f"  Customers: {self.num_customers}")
        print(f"  Brokers: {self.num_brokers}")
        print(f"  Companies: {self.num_companies}")
        print(f"  Securities: {self.num_securities}")
        print(f"  Accounts: {len(self.account_ids)}")
        print(f"  Trades: {self.num_trades}")


def main():
    parser = argparse.ArgumentParser(description='Generate TPC-DI benchmark data')
    parser.add_argument('--scale-factor', '-sf', type=int, default=1,
                        help='Scale factor (1 = ~1000 customers)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility')
    parser.add_argument('--output', '-o', type=str, default='data/raw',
                        help='Output directory')

    args = parser.parse_args()

    output_dir = Path(__file__).parent / args.output

    generator = TPCDIDataGenerator(scale_factor=args.scale_factor, seed=args.seed)
    generator.generate_all(output_dir)


if __name__ == '__main__':
    main()
