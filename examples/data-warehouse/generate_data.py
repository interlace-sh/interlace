#!/usr/bin/env python3
"""Generate sample data for the data warehouse example.

Run this to regenerate data files. On second run, some customer
addresses and segments will change -- triggering SCD Type 2 history.

Usage:
    python generate_data.py           # First run: baseline data
    python generate_data.py --drift   # Simulate changes for SCD2 testing
"""

import argparse
import csv
import random
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Baseline data
# ---------------------------------------------------------------------------

CUSTOMERS = [
    (1, "Alice Johnson", "alice@example.com", "123 Main St, London", "premium"),
    (2, "Bob Smith", "bob.smith@example.com", "45 Oak Ave, Manchester", "standard"),
    (3, "Carol Williams", "carol.w@example.com", "78 Pine Rd, Birmingham", "enterprise"),
    (4, "David Brown", "david.b@example.com", "12 Elm St, Leeds", "standard"),
    (5, "Eva Martinez", "eva.m@example.com", "99 Cedar Ln, Bristol", "premium"),
    (6, "Frank Chen", "frank.chen@example.com", "34 Birch Dr, Edinburgh", "enterprise"),
    (7, "Grace Lee", "grace.lee@example.com", "56 Maple Way, Glasgow", "standard"),
    (8, "Henry Taylor", "henry.t@example.com", "21 Ash Ct, Liverpool", "premium"),
    (9, "Isla Patel", "isla.p@example.com", "67 Spruce St, Cardiff", "standard"),
    (10, "James Wilson", "james.w@example.com", "88 Willow Rd, Belfast", "enterprise"),
    (11, "Kate Murphy", "kate.m@example.com", "15 Hazel Cl, Dublin", "standard"),
    (12, "Liam O'Brien", "liam.ob@example.com", "42 Cherry Ln, Cork", "premium"),
    (13, "Mia Nguyen", "mia.n@example.com", "73 Poplar Ave, Oxford", "standard"),
    (14, "Noah Garcia", "noah.g@example.com", "9 Sycamore Dr, Cambridge", "enterprise"),
    (15, "Olivia Jones", "olivia.j@example.com", "61 Walnut St, Bath", "premium"),
]

PRODUCTS = [
    (101, "Widget A", "electronics", 29.99, 1),
    (102, "Widget B", "electronics", 49.99, 1),
    (103, "Gadget Pro", "electronics", 149.99, 2),
    (104, "Office Chair", "furniture", 299.00, 3),
    (105, "Standing Desk", "furniture", 549.00, 3),
    (106, "Notebook Set", "stationery", 12.50, 4),
    (107, "Premium Pen", "stationery", 24.99, 4),
    (108, "Wireless Mouse", "electronics", 34.99, 1),
    (109, "Keyboard Plus", "electronics", 79.99, 2),
    (110, "Monitor Stand", "furniture", 89.00, 3),
    (111, "Paper Pack", "stationery", 8.99, 5),
    (112, "Desk Lamp", "furniture", 64.50, 5),
]

SUPPLIERS = [
    (1, "TechSupply Co", "tech@supply.com", "UK"),
    (2, "GadgetWorld Ltd", "info@gadgetworld.com", "UK"),
    (3, "FurniturePlus", "sales@furnitureplus.com", "Germany"),
    (4, "StationeryDirect", "orders@stationerydirect.com", "France"),
    (5, "OfficeMart", "hello@officemart.com", "Netherlands"),
]

# Deterministic order data spanning Jan-Jun 2024
ORDERS = [
    (1001, 1, 101, 2, "2024-01-05", 59.98),
    (1002, 2, 104, 1, "2024-01-08", 299.00),
    (1003, 3, 103, 1, "2024-01-10", 149.99),
    (1004, 5, 106, 4, "2024-01-12", 50.00),
    (1005, 1, 108, 1, "2024-01-15", 34.99),
    (1006, 7, 111, 10, "2024-01-18", 89.90),
    (1007, 4, 105, 1, "2024-01-22", 549.00),
    (1008, 6, 102, 3, "2024-01-25", 149.97),
    (1009, 8, 107, 2, "2024-01-28", 49.98),
    (1010, 10, 109, 1, "2024-02-01", 79.99),
    (1011, 3, 112, 2, "2024-02-04", 129.00),
    (1012, 9, 101, 1, "2024-02-07", 29.99),
    (1013, 11, 104, 1, "2024-02-10", 299.00),
    (1014, 2, 110, 1, "2024-02-13", 89.00),
    (1015, 12, 103, 1, "2024-02-16", 149.99),
    (1016, 14, 106, 6, "2024-02-19", 75.00),
    (1017, 1, 105, 1, "2024-02-22", 549.00),
    (1018, 15, 108, 2, "2024-02-25", 69.98),
    (1019, 5, 107, 1, "2024-02-28", 24.99),
    (1020, 13, 102, 1, "2024-03-02", 49.99),
    (1021, 6, 111, 5, "2024-03-05", 44.95),
    (1022, 7, 109, 1, "2024-03-08", 79.99),
    (1023, 4, 112, 1, "2024-03-11", 64.50),
    (1024, 8, 101, 3, "2024-03-14", 89.97),
    (1025, 10, 104, 1, "2024-03-17", 299.00),
    (1026, 3, 108, 1, "2024-03-20", 34.99),
    (1027, 9, 105, 1, "2024-03-23", 549.00),
    (1028, 11, 103, 2, "2024-03-26", 299.98),
    (1029, 2, 106, 3, "2024-03-29", 37.50),
    (1030, 1, 110, 1, "2024-04-01", 89.00),
    (1031, 12, 109, 1, "2024-04-04", 79.99),
    (1032, 14, 107, 2, "2024-04-07", 49.98),
    (1033, 15, 102, 1, "2024-04-10", 49.99),
    (1034, 5, 104, 1, "2024-04-13", 299.00),
    (1035, 13, 112, 1, "2024-04-16", 64.50),
    (1036, 6, 101, 2, "2024-04-19", 59.98),
    (1037, 7, 103, 1, "2024-04-22", 149.99),
    (1038, 4, 111, 8, "2024-04-25", 71.92),
    (1039, 8, 106, 5, "2024-05-01", 62.50),
    (1040, 10, 108, 1, "2024-05-05", 34.99),
    (1041, 3, 105, 1, "2024-05-10", 549.00),
    (1042, 1, 107, 3, "2024-05-15", 74.97),
    (1043, 9, 102, 2, "2024-05-20", 99.98),
    (1044, 11, 110, 1, "2024-05-25", 89.00),
    (1045, 2, 112, 1, "2024-06-01", 64.50),
    (1046, 12, 104, 1, "2024-06-05", 299.00),
    (1047, 14, 101, 1, "2024-06-10", 29.99),
    (1048, 15, 103, 1, "2024-06-15", 149.99),
]

# ---------------------------------------------------------------------------
# Drift mutations -- applied when --drift is passed
# ---------------------------------------------------------------------------

# Customer changes: address moves and segment upgrades
ADDRESS_CHANGES = {
    2: "100 High St, London",      # Bob moved to London
    5: "7 Park Lane, Edinburgh",    # Eva relocated
    9: "33 Queen St, Manchester",   # Isla moved
    13: "18 King Rd, Bristol",      # Mia relocated
}

SEGMENT_CHANGES = {
    4: "premium",      # David upgraded from standard
    7: "enterprise",   # Grace upgraded from standard
    11: "premium",     # Kate upgraded from standard
}

# Product price changes
PRICE_CHANGES = {
    101: 34.99,   # Widget A price increase
    103: 129.99,  # Gadget Pro price decrease
    106: 14.99,   # Notebook Set price increase
}


def write_csv(path: Path, headers: list[str], rows: list[tuple]) -> None:
    """Write rows to a CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  wrote {len(rows)} rows -> {path}")


def generate(*, drift: bool = False) -> None:
    """Generate all CSV files, optionally with drift mutations."""
    print(f"Generating data (drift={'on' if drift else 'off'})...\n")

    # -- Customers --
    customers = []
    for cid, name, email, address, segment in CUSTOMERS:
        if drift and cid in ADDRESS_CHANGES:
            address = ADDRESS_CHANGES[cid]
        if drift and cid in SEGMENT_CHANGES:
            segment = SEGMENT_CHANGES[cid]
        customers.append((cid, name, email, address, segment))

    write_csv(
        DATA_DIR / "customers.csv",
        ["customer_id", "name", "email", "address", "segment"],
        customers,
    )

    # -- Products --
    products = []
    for pid, name, category, price, sid in PRODUCTS:
        if drift and pid in PRICE_CHANGES:
            price = PRICE_CHANGES[pid]
        products.append((pid, name, category, price, sid))

    write_csv(
        DATA_DIR / "products.csv",
        ["product_id", "name", "category", "price", "supplier_id"],
        products,
    )

    # -- Suppliers (no drift) --
    write_csv(
        DATA_DIR / "suppliers.csv",
        ["supplier_id", "name", "contact_email", "country"],
        SUPPLIERS,
    )

    # -- Orders --
    orders = list(ORDERS)
    if drift:
        # Add a few new orders in Jul 2024
        random.seed(42)
        next_id = max(o[0] for o in orders) + 1
        for i in range(5):
            cid = random.choice([c[0] for c in CUSTOMERS])
            pid = random.choice([p[0] for p in PRODUCTS])
            qty = random.randint(1, 4)
            day = random.randint(1, 28)
            price = next(p[3] for p in PRODUCTS if p[0] == pid)
            if pid in PRICE_CHANGES:
                price = PRICE_CHANGES[pid]
            total = round(price * qty, 2)
            orders.append((next_id + i, cid, pid, qty, f"2024-07-{day:02d}", total))

    write_csv(
        DATA_DIR / "orders.csv",
        ["order_id", "customer_id", "product_id", "quantity", "order_date", "total_amount"],
        orders,
    )

    print(f"\nDone. Files written to {DATA_DIR}/")
    if not drift:
        print("Tip: run with --drift to simulate changes for SCD Type 2 testing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate data warehouse sample data")
    parser.add_argument(
        "--drift",
        action="store_true",
        help="Simulate data changes (address moves, segment upgrades, price changes)",
    )
    args = parser.parse_args()
    generate(drift=args.drift)
