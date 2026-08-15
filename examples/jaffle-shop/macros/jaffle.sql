-- dbt writes this five times — once per adapter — because Jinja renders text and the
-- text has to differ per engine:
--
--   {% macro default__cents_to_dollars(column_name) -%}
--       ({{ column_name }} / 100)::numeric(16, 2)
--   {%- endmacro %}
--   {% macro postgres__cents_to_dollars(column_name) -%}
--       ({{ column_name }}::numeric(16, 2) / 100)
--   {%- endmacro %}
--   ... bigquery__, fabric__, and whatever comes next
--
-- Here it is written once. The macro is expanded into the model's AST at compile time
-- and transpiled with everything else, so Postgres gets its own integer-division fix
-- (`CAST(amount AS DOUBLE PRECISION) / NULLIF(100, 0)`) without a second definition.
CREATE MACRO cents_to_dollars(amount) AS (amount / 100)::numeric(16, 2);

-- dbt_utils.generate_surrogate_key: an md5 over the columns, NULLs normalised to a
-- sentinel so ('a', NULL) and (NULL, 'a') do not collide. There is no package to
-- install it from, so it lives here — the same twelve tokens the package ships.
CREATE MACRO surrogate_key(a, b) AS md5(
    concat_ws(
        '-',
        coalesce(cast(a as varchar), '_dbt_utils_surrogate_key_null_'),
        coalesce(cast(b as varchar), '_dbt_utils_surrogate_key_null_')
    )
);
