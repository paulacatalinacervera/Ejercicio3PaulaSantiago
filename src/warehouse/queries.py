from __future__ import annotations

QUERIES: dict[str, dict[str, str]] = {
    "peak_hours": {
        "description": "Horas de mayor demanda y velocidad promedio de la ciudad",
        "sql": """
            SELECT hour,
                   COUNT(*) AS trips,
                   ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
                   ROUND(AVG(duration_min), 2) AS avg_duration_min
            FROM trips
            GROUP BY hour
            ORDER BY trips DESC
        """,
    },
    "critical_zones": {
        "description": "Zonas con mayor volumen y congestion promedio",
        "sql": """
            SELECT origin_zone,
                   origin_zone_name AS zone_name,
                   origin_borough AS borough,
                   COUNT(*) AS trips,
                   ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
                   ROUND(AVG(congestion_index), 4) AS avg_congestion_index
            FROM trips
            GROUP BY origin_zone, origin_zone_name, origin_borough
            ORDER BY trips DESC
            LIMIT 15
        """,
    },
    "borough_flows": {
        "description": "Flujos origen-destino entre boroughs",
        "sql": """
            SELECT oz.borough AS origin_borough,
                   dz.borough AS destination_borough,
                   COUNT(*) AS trips,
                   ROUND(AVG(t.duration_min), 2) AS avg_duration_min
            FROM trips t
            JOIN zones oz ON t.origin_zone = oz.location_id
            JOIN zones dz ON t.destination_zone = dz.location_id
            GROUP BY oz.borough, dz.borough
            ORDER BY trips DESC
            LIMIT 20
        """,
    },
    "peak_vs_offpeak": {
        "description": "Comparacion entre hora pico y valle",
        "sql": """
            SELECT CASE WHEN is_peak THEN 'peak' ELSE 'offpeak' END AS period,
                   COUNT(*) AS trips,
                   ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
                   ROUND(AVG(duration_min), 2) AS avg_duration_min,
                   ROUND(AVG(distance_km), 2) AS avg_distance_km
            FROM trips
            GROUP BY 1
            ORDER BY 1
        """,
    },
    "longest_routes": {
        "description": "Pares origen-destino con mayor duracion promedio (distancia >= 5 km)",
        "sql": """
            SELECT origin_zone,
                   destination_zone,
                   COUNT(*) AS trips,
                   ROUND(AVG(duration_min), 2) AS avg_duration_min,
                   ROUND(AVG(distance_km), 2) AS avg_distance_km
            FROM trips
            GROUP BY origin_zone, destination_zone
            HAVING COUNT(*) >= 3 AND AVG(distance_km) >= 5
            ORDER BY avg_duration_min DESC
            LIMIT 15
        """,
    },
    "day_type_summary": {
        "description": "Demanda y velocidad en semana vs fin de semana",
        "sql": """
            SELECT day_type,
                   COUNT(*) AS trips,
                   ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
                   ROUND(AVG(duration_min), 2) AS avg_duration_min
            FROM trips
            GROUP BY day_type
            ORDER BY day_type
        """,
    },
    "hourly_zone_trend": {
        "description": "Evolucion horaria de una zona critica (Midtown Center, id 161)",
        "sql": """
            SELECT hour,
                   COUNT(*) AS trips,
                   ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
                   ROUND(AVG(congestion_index), 4) AS avg_congestion_index
            FROM trips
            WHERE origin_zone = 161
            GROUP BY hour
            ORDER BY hour
        """,
    },
    "top_od_routes": {
        "description": "Rutas mas usadas de la red",
        "sql": """
            SELECT origin_zone,
                   destination_zone,
                   trips,
                   ROUND(avg_duration_min, 2) AS avg_duration_min,
                   ROUND(avg_distance_km, 2) AS avg_distance_km,
                   peak_trips
            FROM od_edges
            ORDER BY trips DESC
            LIMIT 20
        """,
    },
}
