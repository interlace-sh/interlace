-- @name: quote_categories
-- @materialize: table
-- @strategy: replace
-- @dependencies: quotes

-- Categorize quotes by length
SELECT
    id,
    quote,
    author,
    LENGTH(quote) AS quote_length,
    CASE
        WHEN LENGTH(quote) < 50 THEN 'Short'
        WHEN LENGTH(quote) < 100 THEN 'Medium'
        WHEN LENGTH(quote) < 200 THEN 'Long'
        ELSE 'Very Long'
    END AS length_category
FROM quotes
ORDER BY quote_length DESC;
