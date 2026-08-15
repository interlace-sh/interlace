"""What is left of dbt's macros.

A file whose name starts with `_` is not a model, and a model can import one sitting
next to it — so a macro that does real work becomes an ordinary Python function, and
a model that needs it becomes a dynamic model.

Only worth doing when the macro earns it. `cents_to_dollars` is here because
`stg_supplies` already needs Python for the surrogate key; the other three models that
cast cents just write the cast, because a Python model to spell four tokens is a worse
trade than the repetition.
"""


def cents_to_dollars(column: str) -> str:
    """dbt's project macro, minus the adapter dispatch — this project is DuckDB."""
    return f"({column} / 100)::numeric(16, 2)"


def surrogate_key(columns: list[str]) -> str:
    """`dbt_utils.generate_surrogate_key`: an md5 over the columns, NULLs normalised to
    a sentinel so that (NULL, 'a') and ('a', NULL) do not collide, and every part cast
    to text first. The dbt_utils version is the same expression; there is no package to
    install it from, so it lives here."""
    parts = ", ".join(f"coalesce(cast({c} as varchar), '_dbt_utils_surrogate_key_null_')" for c in columns)
    return f"md5(concat_ws('-', {parts}))"
