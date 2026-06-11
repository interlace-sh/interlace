-- Seed data for the operational Postgres database.
-- Mounted into the postgres container via docker-compose.

CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO customers (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@techcorp.io'),
    ('Carol Lee', 'carol.lee@dataworks.com'),
    ('David Chen', 'dchen@innovate.dev'),
    ('Eva Martinez', 'eva.m@greenfield.org'),
    ('Frank Wu', 'fwu@cloudscale.io'),
    ('Grace Patel', 'gpatel@analytics.co'),
    ('Henry Kim', 'hkim@startup.xyz'),
    ('Isla Brown', 'ibrown@enterprise.com'),
    ('Jack Rivera', 'jrivera@consulting.net');

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    total NUMERIC(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'completed',
    ordered_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO orders (customer_id, total, status) VALUES
    (1, 249.99, 'completed'),
    (2, 89.50, 'completed'),
    (3, 1200.00, 'completed'),
    (1, 45.00, 'completed'),
    (5, 320.75, 'completed'),
    (4, 150.00, 'pending'),
    (7, 780.00, 'completed'),
    (8, 60.25, 'completed'),
    (6, 430.00, 'completed'),
    (9, 95.00, 'completed'),
    (10, 210.50, 'completed'),
    (3, 175.00, 'completed');
