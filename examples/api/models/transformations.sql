-- Transformation models written in SQL
-- These models demonstrate SQL-based transformations with automatic dependency detection

-- @name: user_demographics
-- @materialize: table
-- @strategy: replace
-- @dependencies: users

-- Analyze user demographics and distribution
SELECT
    gender,
    CASE
        WHEN age < 18 THEN 'Under 18'
        WHEN age < 25 THEN '18-24'
        WHEN age < 35 THEN '25-34'
        WHEN age < 45 THEN '35-44'
        WHEN age < 55 THEN '45-54'
        WHEN age < 65 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS user_count,
    AVG(age) AS avg_age,
    MIN(age) AS min_age,
    MAX(age) AS max_age
FROM users
GROUP BY gender, age_group
ORDER BY gender, age_group;


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


-- @name: recipe_nutrition_summary
-- @materialize: table
-- @strategy: replace
-- @dependencies: recipes

-- Summarize recipe nutrition data by cuisine
SELECT
    cuisine,
    COUNT(*) AS recipe_count,
    AVG(calories_per_serving) AS avg_calories,
    AVG(servings) AS avg_servings,
    AVG(prep_time_minutes) AS avg_prep_time,
    AVG(cook_time_minutes) AS avg_cook_time
FROM recipes
GROUP BY cuisine
ORDER BY recipe_count DESC;

