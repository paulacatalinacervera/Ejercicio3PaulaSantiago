param(
    [switch]$Local,
    [int]$Trips = 500000,
    [string]$StartDate = "2026-11-14",
    [int]$Days = 6,
    [int]$Limit = 200000,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop no esta iniciado. Inicialo y vuelve a ejecutar el script."
}

$composeArgs = @("compose")
if ($Local) { $composeArgs += @("--profile", "local") }
$composeArgs += @("up", "-d")
if (-not $SkipBuild) { $composeArgs += "--build" }
Write-Host "Levantando infraestructura: docker $($composeArgs -join ' ')"
& docker @composeArgs

Write-Host "`nInicializando servicios..."
python scripts/bootstrap.py --create-bucket

Write-Host "`nGenerando datos sinteticos..."
python -m src.generator --trips $Trips --start-date $StartDate --days $Days

Write-Host "`nProcesando agregados..."
python -m src.batch

Write-Host "`nSubiendo a S3/MinIO..."
python -m src.s3_utils --upload --create-bucket

Write-Host "`nCargando grafo Neo4j..."
python -m src.graph.load_neo4j --clear

Write-Host "`nCalculando centralidad y ruta mas corta..."
python -m src.graph.run_gds

Write-Host "`nEjecutando consultas analiticas locales (DuckDB)..."
python -m src.warehouse.bigquery_load --local

Write-Host @"

Listo. Para la demostracion en vivo abre tres terminales:

1) Spark Structured Streaming:
   docker compose exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/mobility/src/spark/streaming_windows.py

2) Productor Kafka:
   python -m src.producer --limit $Limit --rate 2000

3) Dashboard:
   streamlit run src/dashboard/app.py

Consultas rapidas:
   docker compose exec cassandra cqlsh -e "SELECT * FROM mobility.city_congestion_by_window LIMIT 5;"
   Abre http://localhost:7474 para Neo4j Browser y ejecuta: MATCH (z:Zone) RETURN z ORDER BY z.pagerank DESC LIMIT 10;
"@
