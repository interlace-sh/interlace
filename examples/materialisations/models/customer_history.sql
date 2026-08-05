/* interlace: { strategy: scd_type_2, key: id } */
-- virtual + scd_type_2: keyed history with validity windows. The target carries
-- the query's columns plus _valid_from / _valid_to; change a tier and re-run and
-- the old version closes while the new one opens.
SELECT id, tier FROM customers
