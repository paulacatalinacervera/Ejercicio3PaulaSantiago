# Reto 3 - Monitoreo de movilidad urbana con datos de sensores

Arquitectura de datos masivos para una ciudad que recibe lecturas GPS de taxis, buses y bicicletas públicas. El objetivo es detectar congestión, rutas críticas y estaciones/zoras clave de la red usando datos sintéticos reproducibles construidos a partir del catálogo oficial de zonas de NYC.

## 1. Problema y preguntas de negocio

Una ciudad necesita responder en tiempo casi real:

1. ¿Qué zonas están más congestionadas en cada ventana de tiempo?
2. ¿Cuáles son las horas pico y cómo cambia la velocidad promedio?
3. ¿Qué zonas son más importantes dentro de la red de transporte (PageRank)?
4. ¿Qué zonas actúan como puentes o intermediarias entre otras (betweenness)?
5. ¿Cuál es la ruta más corta/rapida entre dos zonas?
6. ¿Qué pares origen-destino concentran más viajes y cuáles son más lentos?

El volumen mínimo exigido es de 500.000 registros. La configuración por defecto genera **500.000 viajes y 2.748.771 lecturas GPS** (4 a 7 pings por viaje) repartidos en **6 días del 14 al 19 de noviembre de 2026**, validado por `generator_metadata.json` y por `scripts/smoke_test.py`. Así se cumple el mínimo tanto por viajes como por registros GPS.

## 2. Arquitectura

```text
                    +-----------------------------+
                    | Generador sintetico Python |
                    |  (zonas NYC + semilla fija)|
                    +-------------+---------------+
                                  |
             +--------------------+---------------------+
             |                                          |
             v                                          v
   trips.parquet / gps_readings.parquet          S3 / MinIO
             |                                   raw/synthetic/
             |                                          |
             |                                          v
             |                                   processed/trips/
             |                                   processed/trips_flat/
             |                                   processed/hourly_zone_stats/
             |                                   processed/od_edges/
             |                                          |
             |                                          v
              |                                   GCS -> BigQuery
              |                                   LOAD DATA + SQL
             |                                          |
             v                                          v
      Kafka topic mobility.gps                  Streamlit historicos
             |
             v
   Spark Structured Streaming
      - ventanas de 1 minuto
      - watermark 2 minutos
             |
      +------+------+
      |             |
      v             v
 Cassandra      S3 / MinIO
 series por     streaming_windows/
 vehiculo y
 congestion
      |
      v
 Streamlit en vivo

   trips/od_edges
        |
        v
   Neo4j + GDS
   PageRank / betweenness / Dijkstra
        |
        v
   Streamlit grafo + Neo4j Browser
```

### Tecnologías y justificación

| Tecnología | Uso | Por qué |
|---|---|---|
| Kafka | Ingesta de lecturas GPS continuas | Desacopla productores y consumidores, permite replay y tolera picos de tráfico. |
| Spark Structured Streaming | Ventanas de tiempo, watermark y agregaciones | Procesamiento distribuido con semántica de event-time y tolerancia a fallos. |
| Cassandra | Series de tiempo por vehículo y congestión por zona | Escritura rápida de alto volumen, modelo query-first y particiones acotadas. |
| Neo4j + GDS | Red de zonas, PageRank, intermediación y Dijkstra | Consultas de grafo nativas y librería GDS para centralidad y rutas. |
| S3 / MinIO | Data lake de históricos y agregados | Almacenamiento barato, Parquet columnar y particionado para analítica. |
| GCS + BigQuery | Análisis de horas pico y zonas críticas | Parquet en GCS como staging y consultas SQL escalables sin administrar infraestructura. |
| DuckDB | Modo local sin credenciales cloud | Permite demostrar el análisis histórico sin cuenta de BigQuery. |
| Streamlit | Dashboard de demostración | Panel oscuro con resumen del dataset, mapa NYC de nodos y corredores, tabla numérica de zonas, congestión en vivo, grafo y analítica histórica. |

El recorrido completo del código y los datos está descrito en las secciones siguientes y en los docstrings de cada módulo.

Alternativas descartadas: HDFS (más operación que S3), MongoDB (no optimizado para series de tiempo particionadas como Cassandra), Neo4j para analítica masiva (se usa solo para el grafo), Snowflake (credenciales de la cuenta no disponibles durante la validación).

## 3. Datos

- `data/taxi_zone_lookup.csv`: catálogo público de 265 zonas de NYC (LocationID, Borough, Zone, service_zone). Es la dimensión de estaciones/zonas.
- Generador sintético reproducible: `src/generator.py`.
  - Semilla configurable (`--seed`, por defecto 42).
  - Modelo de gravedad origen-destino sobre las 265 zonas.
  - Perfil horario con picos 7–9 a. m. y 5–7 p. m.; variación fin de semana.
  - Flota simulada de 5.000 vehículos.
  - Velocidad dependiente de borough y hora; índice de congestión = `1 - velocidad/35`.
  - Varios pings GPS por viaje, con latitud/longitud interpoladas.
  - Coordenadas: si existe `data/zone_centroids.csv` se usan centroides reales; si no, se generan coordenadas deterministas por borough para que el demo funcione sin internet.
- Salidas:
  - `data/synthetic/trips.parquet`
  - `data/synthetic/gps_readings.parquet`
  - `data/synthetic/gps_readings_sample.csv`
  - `data/synthetic/generator_metadata.json`
- Volumen por defecto: 500.000 viajes en 6 días, entre 2.000.000 y 3.500.000 lecturas.
- Licencia: los datos de zonas son de NYC TLC (dominio público). Los viajes y lecturas son sintéticos y se generan con el código de este repositorio.

Para usar centroides reales:

```powershell
pip install geopandas
python scripts/fetch_zone_geometry.py
```

Si no se ejecuta, el generador usa coordenadas sintéticas por borough y lo documenta en el metadata.

## 4. Estructura del repositorio

```text
.
├── README.md
├── docker-compose.yml
├── Dockerfile.spark
├── Dockerfile.dashboard
├── .env.example
├── requirements-data.txt
├── requirements-dashboard.txt
├── data/
│   └── taxi_zone_lookup.csv
├── src/
│   ├── config.py
│   ├── zones.py
│   ├── generator.py
│   ├── batch.py
│   ├── producer.py
│   ├── s3_utils.py
│   ├── cassandra_utils.py
│   ├── spark/streaming_windows.py
│   ├── graph/load_neo4j.py
│   ├── graph/run_gds.py
│   ├── warehouse/queries.py
│   ├── warehouse/local_duckdb.py
│   ├── warehouse/bigquery_load.py
│   └── dashboard/app.py
├── sql/
│   ├── cassandra_schema.cql
│   ├── neo4j_constraints.cypher
│   ├── neo4j_gds.cypher
│   ├── bigquery_setup.sql
│   └── analysis_queries.sql
├── scripts/
│   ├── bootstrap.py
│   ├── smoke_test.py
│   ├── fetch_zone_geometry.py
│   └── run_demo.ps1
└── tests/
```

## 5. Requisitos

- Docker Desktop iniciado (el compose usa Kafka, Cassandra, Neo4j, Spark, MinIO y Streamlit).
- Python 3.11 o superior en el host.
- 8 GB de RAM libres recomendados (Cassandra + Neo4j + Spark + Kafka).
- Opcional para la nube: cuenta AWS con bucket S3 y cuenta GCP con GCS y BigQuery.

Instala dependencias del host:

```powershell
python -m pip install -r requirements-data.txt
python -m pip install -r requirements-dashboard.txt
```

## 6. Configuración

Copia el archivo de ejemplo:

```powershell
Copy-Item .env.example .env
```

Modo local (sin AWS/BigQuery): deja `LOCAL_MODE=true`, `S3_ENDPOINT_URL=http://localhost:9000` y las credenciales de MinIO (`minioadmin`/`minioadmin`).

Modo AWS real:

```dotenv
LOCAL_MODE=false
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
S3_BUCKET=tu-bucket-unico
S3_PREFIX=mobility
S3_ENDPOINT_URL=
S3_ENDPOINT_URL_DOCKER=
```

Modo GCP + BigQuery con ADC de gcloud:

```dotenv
GCP_PROJECT_ID=pausan-proyecto
GCP_DATASET=mobility
GCP_LOCATION=US
GCS_BUCKET=proyecto-data-2026-mobility
GCS_PREFIX=mobility
GOOGLE_APPLICATION_CREDENTIALS=
```

Si prefieres una service account, coloca la ruta absoluta del JSON en `GOOGLE_APPLICATION_CREDENTIALS`. `load_dotenv` la exporta antes de crear los clientes de Google Cloud.

No subas `.env` al repositorio. `.gitignore` ya lo excluye.

## 7. Ejecución rápida (modo local)

1. Inicia Docker Desktop.
2. Levanta la infraestructura con MinIO:
```powershell
docker compose --profile local up -d --build
```
3. Inicializa Kafka, Cassandra, Neo4j y el bucket local:
```powershell
python scripts/bootstrap.py --create-bucket
```
4. Genera los datos sintéticos:
```powershell
python -m src.generator --trips 500000 --start-date 2026-11-14 --days 6
```
5. Enriquece y crea agregados para S3/GCS/BigQuery:

```powershell
python -m src.batch
```
6. Sube los datos a MinIO (API compatible con S3):
```powershell
python -m src.s3_utils --upload --create-bucket
```
7. Carga el grafo y calcula centralidad:
```powershell
python -m src.graph.load_neo4j --clear
python -m src.graph.run_gds
```

8. Ejecuta las consultas históricas sin cuenta BigQuery:

```powershell
python -m src.warehouse.bigquery_load --local
```
Los resultados quedan en `results/duckdb_*.csv`.
También puedes automatizar los pasos 2 a 8:
```powershell
.\scripts\run_demo.ps1 -Local
```

## 8. Demo en vivo (streaming)

Abre tres terminales.

Terminal 1 — job Spark Structured Streaming:

```powershell
docker compose exec spark spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
  /opt/mobility/src/spark/streaming_windows.py
```

La primera ejecución descarga los conectores de Maven Central. Si el contenedor no tiene internet, ejecuta el comando una vez con red y luego reutiliza la caché de Ivy.

Terminal 2 — productor Kafka:

```powershell
python -m src.producer --limit 200000 --rate 2000
```

Terminal 3 — dashboard:

```powershell
streamlit run src/dashboard/app.py
```

Abre `http://localhost:8501`. El panel usa un tema oscuro fijo definido en `.streamlit/config.toml` y tiene cinco pestañas:

- **Resumen**: KPIs del dataset, distribución por borough y ficha técnica con las fuentes S3, GCS y BigQuery.
- **Mapa de red**: zonas geolocalizadas sobre NYC, corredores origen-destino, etiquetas de las zonas más relevantes y tabla numérica seleccionable. El ancho de línea representa viajes y el color la duración media. El trazado es una aproximación tipo cuadrícula (avenida + calle) sobre centroides, no ruteo real calle por calle.
- **Congestión en vivo**: última ventana de Cassandra con auto-refresh de 5 s.
- **Grafo y rutas**: PageRank, intermediación y Dijkstra desde Neo4j.
- **Analítica histórica**: las 8 consultas canónicas sobre DuckDB.

`data/processed/od_links.parquet` se genera en `python -m src.batch` y contiene los extremos de cada corredor, nombres de zona, borough, distancia haversine, distancia tipo cuadrícula y los waypoints del trazado.

Consultas de demostración:

```powershell
# Cassandra: ventanas de congestión
docker compose exec cassandra cqlsh -e "SELECT * FROM mobility.city_congestion_by_window LIMIT 5;"

# Cassandra: últimas lecturas de un vehículo
docker compose exec cassandra cqlsh -e "SELECT * FROM mobility.sensor_readings_by_vehicle WHERE vehicle_id='veh-00042' AND day_bucket='20261113' LIMIT 10;"
```

En Neo4j Browser (`http://localhost:7474`) puede aparecer la pantalla de conexión con el aviso `Database access not available. Please use :server connect to establish connection.` Completa así:

- **Connect URL**: `bolt://localhost:7687` (desde el host; dentro del dashboard el contenedor usa `bolt://neo4j:7687`).
- **Username**: `neo4j`
- **Password**: `mobility123`
- **Authentication type**: `Username / Password`

Luego haz clic en **Connect**. Si aparece un campo **database**, deja `neo4j`. Si el aviso persiste, escribe `:server connect` en la barra `>` y repite los mismos datos.

Consulta de demostración:

```cypher
MATCH (z:Zone)
RETURN z.zone, z.borough, z.pagerank, z.betweenness
ORDER BY z.pagerank DESC
LIMIT 10;
```

Ruta más corta ponderada:

```cypher
MATCH (s:Zone {location_id: 161}), (t:Zone {location_id: 132})
CALL gds.shortestPath.dijkstra.stream('zoneGraph', {
  sourceNode: id(s), targetNode: id(t),
  relationshipWeightProperty: 'avg_duration_min'
})
YIELD path, totalCost
RETURN [n IN nodes(path) | n.zone] AS ruta, totalCost;
```

## 9. Modelo Cassandra

Archivo: `sql/cassandra_schema.cql`.

### `sensor_readings_by_vehicle`

```text
PRIMARY KEY ((vehicle_id, day_bucket), event_time, reading_id)
CLUSTERING ORDER BY (event_time DESC, reading_id ASC)
```

- **Partición:** `(vehicle_id, day_bucket)`.
- Justificación: una partición por vehículo-día queda acotada (máximo ~1.440 lecturas si se emite una por minuto). Si se particionara solo por `vehicle_id`, la partición crecería sin límite y podría convertirse en una partición gigante/hot partition. Si se particionara solo por día, las consultas por vehículo harían un scan innecesario.
- **Clustering:** `event_time DESC` responde “últimas N lecturas de un vehículo” sin ordenar en memoria.

### `congestion_by_zone_window`

```text
PRIMARY KEY ((zone_id, day_bucket), window_start)
```

- Consultas del dashboard por zona y día.
- `window_start DESC` devuelve primero la ventana más reciente.
- El dashboard lee las particiones `((day_bucket, window_start), zone_id)` y filtra el día en Pandas; no usa `ALLOW FILTERING`.

### `city_congestion_by_window`

```text
PRIMARY KEY ((day_bucket, window_start), zone_id)
```

- Permite obtener el ranking ciudad-wide de una ventana sin ejecutar 265 consultas.
- La partición contiene hasta 265 filas por ventana, un tamaño manejable.

## 10. Modelo Neo4j

- Nodo `Zone`: `location_id`, `zone`, `borough`, `service_zone`, `lat`, `lon`, `pagerank`, `betweenness`.
- Relación `TRIP`: `trips`, `avg_duration_min`, `avg_distance_km`, `peak_trips`.
- Restricción de unicidad en `Zone.location_id`.

Algoritmos (`src/graph/run_gds.py`):

- **PageRank** ponderado por cantidad de viajes: identifica zonas que reciben flujos desde zonas importantes.
- **Betweenness** ponderada por duración promedio: identifica zonas puente. Si la versión de GDS no soporta peso, el script hace fallback a intermediación no ponderada.
- **Dijkstra** ponderado por duración promedio: ruta más rápida entre dos zonas.

## 11. S3, GCS y BigQuery

### Organización en S3

```text
s3://<bucket>/mobility/
├── raw/synthetic/
│   ├── trips.parquet
│   ├── gps_readings.parquet
│   └── generator_metadata.json
├── processed/
│   ├── trips/                 # Parquet particionado por pickup_date
│   ├── trips_flat/trips.parquet
│   ├── hourly_zone_stats.parquet
│   ├── od_edges.parquet
│   ├── od_links.parquet
│   └── zones.parquet
├── streaming_windows/         # salida del job Spark
└── reference/taxi_zone_lookup.csv
```

Formato Parquet con compresión Snappy. Se eligió Parquet por ser columnar, comprimible y con predicate pushdown; las particiones por fecha permiten limitar lecturas en consultas históricas.

### BigQuery

1. Crea un proyecto GCP con facturación y habilita las APIs de BigQuery y Cloud Storage.
2. Ejecuta `gcloud auth application-default login` o completa `GOOGLE_APPLICATION_CREDENTIALS` en `.env`.
3. Completa `GCP_PROJECT_ID`, `GCP_DATASET`, `GCP_LOCATION`, `GCS_BUCKET` y `GCS_PREFIX` en `.env`.
4. Carga los Parquet a GCS, crea el dataset, carga las tablas y ejecuta las consultas:

```powershell
python -m src.warehouse.bigquery_load
```

El script sube `zones.parquet`, `trips_flat/trips.parquet`, `hourly_zone_stats.parquet` y `od_edges.parquet` a `gs://<bucket>/mobility/processed/`, crea el dataset `mobility` en `US`, carga las tablas `zones`, `trips`, `hourly_zone_stats` y `od_edges` con `WRITE_TRUNCATE`, y guarda los resultados en `results/bigquery_*.csv`.

Para reutilizar archivos ya subidos usa `--skip-upload`. Para descargar primero desde S3 usa `--from-s3`.

### Consultas analíticas

Definidas en `src/warehouse/queries.py` y `sql/analysis_queries.sql`:

1. `peak_hours`: horas de mayor demanda y velocidad promedio.
2. `critical_zones`: zonas con más viajes y mayor congestión.
3. `borough_flows`: flujos origen-destino entre boroughs.
4. `peak_vs_offpeak`: comparación pico vs. valle.
5. `longest_routes`: pares origen-destino con mayor duración promedio.
6. `day_type_summary`: semana vs. fin de semana.
7. `hourly_zone_trend`: evolución horaria de Midtown Center (zona 161).
8. `top_od_routes`: rutas más usadas de la red.

## 12. Pruebas y validación

```powershell
pytest -q
python scripts/smoke_test.py
```

`smoke_test.py` verifica archivos generados, puertos, topic de Kafka, tablas de Cassandra, grafo de Neo4j y acceso al bucket. Las pruebas unitarias validan volumen, esquema, reproducibilidad y claves de partición.

Evidencia de escala:

```powershell
python -c "import json; print(json.load(open('data/synthetic/generator_metadata.json'))['readings'])"
python -c "import json; print(json.load(open('results/batch_summary.json')))"
```

Con la configuración por defecto se generan 500.000 viajes y 2.748.771 registros GPS en 6 fechas distintas, y se procesan en ventanas de 1 minuto con watermark de 2 minutos.

## 13. Rendimiento esperado y límites

- Generación de 500.000 viajes en 6 días y ~2,75M de lecturas: unos minutos en un equipo de 12 CPUs.
- Cassandra: escritura por micro-batch cada 10 segundos; se ajustan `batch.size.rows` y `concurrent.writes` para evitar saturación.
- Spark: `spark.sql.shuffle.partitions=8` para un dataset de demo; en producción se escalaría según volumen.
- Neo4j: 265 nodos y decenas de miles de relaciones agregadas; PageRank e intermediación son rápidos. Para millones de nodos se requeriría Neo4j Enterprise/Aura o GraphFrames.
- BigQuery: carga de Parquet desde GCS y consultas sobre tablas pequeñas/medianas; el tier gratuito cubre el volumen de la demo.
- Limitaciones: los viajes son sintéticos, no hay red vial real (se interpolan coordenadas), y la demo local usa MinIO/DuckDB cuando no hay credenciales cloud.

## 14. Solución de problemas

- **Docker no inicia:** abre Docker Desktop y espera a que el daemon esté listo. `docker info` debe responder.
- **Cassandra tarda:** el primer arranque puede tardar 60–90 segundos. `scripts/bootstrap.py` espera el puerto.
- **Neo4j no carga GDS:** verifica `NEO4J_PLUGINS` y la conexión a internet del contenedor.
- **Neo4j Browser dice `Database access not available`:** el Browser está desconectado. Conecta con `bolt://localhost:7687`, usuario `neo4j`, contraseña `mobility123` y autenticación `Username / Password`; o ejecuta `:server connect`.
- **Cassandra devuelve `ALLOW FILTERING`:** usa el dashboard actualizado o consulta con la clave de partición completa, por ejemplo `WHERE day_bucket='20261114' AND window_start='2026-11-14 00:28:00+0000'`.
- **Spark no encuentra conectores:** ejecuta el `spark-submit` con `--packages` y conexión a internet; la primera vez descarga dependencias.
- **BigQuery falla por credenciales:** ejecuta `gcloud auth application-default login` o revisa `GOOGLE_APPLICATION_CREDENTIALS`; también puedes usar `python -m src.warehouse.bigquery_load --local`.
- **Puertos ocupados:** 9092, 9042, 7474, 7687, 8501, 9000 y 9001 deben estar libres.
- **No hay ventanas en el dashboard:** confirma que el productor y el job Spark estén corriendo y que el día simulado coincida con `SIM_DAY`.

## 15. Demo sugerida (10 minutos)

1. Mostrar el diagrama de arquitectura y explicar por qué cada tecnología.
2. Ejecutar `python -m src.generator` y mostrar el volumen en `generator_metadata.json`.
3. Mostrar S3/MinIO con las carpetas `raw/` y `processed/`.
4. Lanzar el productor y el job Spark; mostrar las ventanas llegando a Cassandra.
5. Abrir Streamlit y mostrar congestión en vivo.
6. Ejecutar en Neo4j Browser el top de PageRank y una ruta Dijkstra.
7. Ejecutar una consulta SQL de horas pico en BigQuery o DuckDB.
8. Cerrar con limitaciones y trabajo futuro.

## 16. Integrantes

Completar con los nombres del grupo y la distribución de trabajo antes de la entrega.
