#!/usr/bin/env python3
"""Push one round of changes into the source Postgres.

Each statement commits on its own, so the logical slot sees an insert, an
update, and (the first time) a delete. `interlace serve` appends them to the
orders stream and rebuilds the replica.

    python push.py

Running it again upserts order 6, toggles order 5, and finds order 3 already
gone. Point it elsewhere with SOURCE_PG_DSN — and the same value in
interlace.yaml, which is what the daemon reads.
"""

from __future__ import annotations

import os

DSN = os.environ.get("SOURCE_PG_DSN", "postgresql://interlace:interlace@localhost:5457/shop")


def main() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders (id, customer, amount, status)
            VALUES (6, 'Margaret Hamilton', 88.00, 'pending')
            ON CONFLICT (id) DO UPDATE
                SET amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    updated_at = now()
            """
        )
        print(f"upserted order 6 ({cur.rowcount} row)")

        cur.execute(
            """
            UPDATE orders
            SET status = CASE WHEN status = 'paid' THEN 'refunded' ELSE 'paid' END,
                updated_at = now()
            WHERE id = 5
            """
        )
        print(f"toggled order 5 ({cur.rowcount} row)")

        cur.execute("DELETE FROM orders WHERE id = 3 RETURNING id")
        deleted = cur.fetchall()
        print("deleted order 3" if deleted else "order 3 already gone")

    print("changes are in Postgres — the replica follows within a few seconds")


if __name__ == "__main__":
    main()
