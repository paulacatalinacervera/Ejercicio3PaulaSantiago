-- Keyspace de operacion para series de tiempo de movilidad.
CREATE KEYSPACE IF NOT EXISTS mobility
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Serie de tiempo por vehiculo.
-- Partition key: (vehicle_id, day_bucket).
-- Justificacion: una particion por vehiculo-dia queda acotada (maximo ~1440
-- lecturas si se emite una por minuto) y evita particiones gigantes que
-- crecerian sin limite si solo se particionara por vehicle_id.
-- Clustering: event_time DESC para responder "ultimas N lecturas del vehiculo".
CREATE TABLE IF NOT EXISTS mobility.sensor_readings_by_vehicle (
    vehicle_id text,
    day_bucket text,
    event_time timestamp,
    reading_id text,
    zone_id int,
    lat double,
    lon double,
    speed_kmh double,
    occupancy int,
    trip_id text,
    PRIMARY KEY ((vehicle_id, day_bucket), event_time, reading_id)
) WITH CLUSTERING ORDER BY (event_time DESC, reading_id ASC);

-- Congestion agregada por zona y ventana.
-- Partition key: (zone_id, day_bucket) para consultas del dashboard por zona.
-- Clustering: window_start DESC para obtener la ventana mas reciente primero.
CREATE TABLE IF NOT EXISTS mobility.congestion_by_zone_window (
    zone_id int,
    day_bucket text,
    window_start timestamp,
    window_end timestamp,
    readings bigint,
    vehicles bigint,
    avg_speed_kmh double,
    avg_occupancy double,
    congestion_index double,
    PRIMARY KEY ((zone_id, day_bucket), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Vista ciudad-wide por ventana.
-- Partition key: (day_bucket, window_start) para ranking de todas las zonas
-- de una ventana sin ejecutar 265 consultas independientes.
CREATE TABLE IF NOT EXISTS mobility.city_congestion_by_window (
    day_bucket text,
    window_start timestamp,
    zone_id int,
    readings bigint,
    avg_speed_kmh double,
    congestion_index double,
    PRIMARY KEY ((day_bucket, window_start), zone_id)
) WITH CLUSTERING ORDER BY (zone_id ASC);
