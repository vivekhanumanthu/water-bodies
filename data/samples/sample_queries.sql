-- Dashboard 1: Data quality
SELECT crash_year, crash_month, COUNT(*) AS rows
FROM crashes_clean
GROUP BY crash_year, crash_month
ORDER BY crash_year, crash_month;

-- Dashboard 2: Model readiness features
SELECT weather_condition, COUNT(*) AS crashes,
       SUM(CASE WHEN CAST(injuries_fatal AS INT) > 0 THEN 1 ELSE 0 END) AS fatal_crashes
FROM crashes_clean
GROUP BY weather_condition
ORDER BY crashes DESC;

-- Dashboard 3: Business insights
SELECT prim_contributory_cause, COUNT(*) AS crashes
FROM crashes_clean
GROUP BY prim_contributory_cause
ORDER BY crashes DESC
LIMIT 20;

-- Dashboard 4: Scalability (read from reports/scaling_cost_performance.csv in Tableau)
