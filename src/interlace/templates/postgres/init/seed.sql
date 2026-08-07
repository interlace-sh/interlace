-- Seeds the source database (runs once, on first `docker compose up`). A small
-- orders table with an `updated_at` column — the cursor the incremental pull follows.
CREATE TABLE orders (
    id          bigint PRIMARY KEY,
    customer    text NOT NULL,
    amount      numeric(10, 2) NOT NULL,
    status      text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO orders (id, customer, amount, status, updated_at) VALUES
    (1, 'Ada Lovelace',    49.90,  'paid',     '2026-01-01 09:00:00+00'),
    (2, 'Alan Turing',    129.00,  'paid',     '2026-01-01 10:15:00+00'),
    (3, 'Grace Hopper',    19.99,  'refunded', '2026-01-01 11:20:00+00'),
    (4, 'Katherine Johnson', 74.50, 'paid',    '2026-01-02 08:05:00+00'),
    (5, 'Edsger Dijkstra',  0.00,  'pending',  '2026-01-02 09:30:00+00');

-- Try incrementality: after the first `interlace apply`, touch a row and re-apply —
-- only the changed row is re-pulled (the model resumes from the max updated_at).
--   UPDATE orders SET status = 'paid', updated_at = now() WHERE id = 5;
