-- Source table, publication, and logical slot. The slot is created BEFORE the
-- seed inserts, so those inserts sit in the slot and land when `interlace serve`
-- connects. A primary key is the replica identity: a delete carries `id`, which
-- is enough for the replica model to drop the row.
CREATE TABLE orders (
    id          bigint PRIMARY KEY,
    customer    text NOT NULL,
    amount      numeric(10, 2) NOT NULL,
    status      text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE PUBLICATION orders_pub FOR TABLE public.orders;

SELECT pg_create_logical_replication_slot('interlace_orders', 'pgoutput');

INSERT INTO orders (id, customer, amount, status, updated_at) VALUES
    (1, 'Ada Lovelace',       49.90, 'paid',     '2026-01-01 09:00:00+00'),
    (2, 'Alan Turing',       129.00, 'paid',     '2026-01-01 10:15:00+00'),
    (3, 'Grace Hopper',       19.99, 'refunded', '2026-01-01 11:20:00+00'),
    (4, 'Katherine Johnson',  74.50, 'paid',     '2026-01-02 08:05:00+00'),
    (5, 'Edsger Dijkstra',     0.00, 'pending',  '2026-01-02 09:30:00+00');
