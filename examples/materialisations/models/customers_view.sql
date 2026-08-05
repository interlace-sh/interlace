/* interlace: { materialise: view } */
-- view: the physical layer is a VIEW over the query (no data copied); still
-- fronted by an environment view, so it promotes and rolls back like any model.
SELECT id, name FROM customers
