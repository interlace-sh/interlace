-- @name: no_negative_quantities
-- @model: orders
-- @severity: error
-- @description: Orders should never have negative quantities
SELECT order_id, quantity
FROM orders
WHERE quantity < 0
