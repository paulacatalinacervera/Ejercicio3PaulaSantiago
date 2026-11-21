-- Consultas de negocio del Reto 3. Se ejecutan desde src/warehouse/queries.py
-- o manualmente en la consola de BigQuery.

-- 1. Horas pico
SELECT hour,
       COUNT(*) AS trips,
       ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
       ROUND(AVG(duration_min), 2) AS avg_duration_min
FROM trips
GROUP BY hour
ORDER BY trips DESC;

-- 2. Zonas criticas
SELECT origin_zone,
       origin_zone_name AS zone_name,
       origin_borough AS borough,
       COUNT(*) AS trips,
       ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
       ROUND(AVG(congestion_index), 4) AS avg_congestion_index
FROM trips
GROUP BY origin_zone, origin_zone_name, origin_borough
ORDER BY trips DESC
LIMIT 15;

-- 3. Flujos entre boroughs
SELECT oz.borough AS origin_borough,
       dz.borough AS destination_borough,
       COUNT(*) AS trips,
       ROUND(AVG(t.duration_min), 2) AS avg_duration_min
FROM trips t
JOIN zones oz ON t.origin_zone = oz.location_id
JOIN zones dz ON t.destination_zone = dz.location_id
GROUP BY oz.borough, dz.borough
ORDER BY trips DESC
LIMIT 20;

-- 4. Pico vs valle
SELECT CASE WHEN is_peak THEN 'peak' ELSE 'offpeak' END AS period,
       COUNT(*) AS trips,
       ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
       ROUND(AVG(duration_min), 2) AS avg_duration_min
FROM trips
GROUP BY 1
ORDER BY 1;

-- 5. Rutas con mayor duracion promedio (distancia >= 5 km)
SELECT origin_zone,
       destination_zone,
       COUNT(*) AS trips,
       ROUND(AVG(duration_min), 2) AS avg_duration_min,
       ROUND(AVG(distance_km), 2) AS avg_distance_km
FROM trips
GROUP BY origin_zone, destination_zone
HAVING COUNT(*) >= 3 AND AVG(distance_km) >= 5
ORDER BY avg_duration_min DESC
LIMIT 15;

-- 6. Semana vs fin de semana
SELECT day_type,
       COUNT(*) AS trips,
       ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
       ROUND(AVG(duration_min), 2) AS avg_duration_min
FROM trips
GROUP BY day_type
ORDER BY day_type;

-- 7. Tendencia horaria de una zona critica
SELECT hour,
       COUNT(*) AS trips,
       ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
       ROUND(AVG(congestion_index), 4) AS avg_congestion_index
FROM trips
WHERE origin_zone = 161
GROUP BY hour
ORDER BY hour;
