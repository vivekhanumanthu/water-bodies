-- Basic row and geometry profile
SELECT geometry_type, COUNT(*) AS records
FROM water_bodies_curated
GROUP BY geometry_type
ORDER BY records DESC;

-- Null profile checks (illustrative)
SELECT *
FROM water_bodies_curated
LIMIT 100;
