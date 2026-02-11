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

