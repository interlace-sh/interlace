-- virtual + full (the defaults): an interlace-owned snapshot table, rebuilt
-- whole (CREATE OR REPLACE) and read through an environment view. Every model
-- below reads this one.
SELECT id, name, tier, day FROM seed
