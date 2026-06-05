# GPS Pipeline — Evaluación Técnica

Pipeline GPS en tiempo real + ingesta batch de CSV de mantenimiento sobre AWS, reproducible localmente con LocalStack.

---

## Mapeo de requisitos PDF → implementación

| Requisito PDF | Implementación |
|---|---|
| Ingesta GPS en tiempo real | Kinesis Data Streams → Lambda `validate_gps` |
| Ingesta CSV diaria de mantenimiento | S3 PutObject → Lambda `ingest_maintenance` |
| Validación: coordenadas, timestamp, duplicados | bbox Áncash, fresco <1h, DynamoDB conditional write |
| Detección pérdida de señal >10 min | Lambda `detect_signal_loss` (EventBridge c/5 min) + SNS |
| Almacenamiento histórico | S3 medallion bronze/silver/gold, Parquet Snappy |
| Dashboard | Athena + QuickSight IaC; Streamlit para demo local |
| Alertas | SNS topic `gps-alertas` |
| Robustez: retry, DLQ, logging, alarmas | SQS DLQ, bisectBatchOnFunctionError, JSON CloudWatch, 5 alarmas |
| Reproducibilidad | LocalStack + Docker Compose + Terraform (un comando) |
| Calidad de datos | Pandera schemas declarativos, métricas a gold/ |

---

## Mapeo de campos PDF → código

### Evento GPS (streaming)

| Campo PDF | Campo interno | Transformación |
|---|---|---|
| `equipo_id` | `equipo_id` | igual |
| `timestamp` | `timestamp` | igual (ISO 8601 UTC) |
| `latitud` | `latitude` | renombrado en `_normalize_fields()` |
| `longitud` | `longitude` | renombrado en `_normalize_fields()` |
| `velocidad` | `speed_kmh` | renombrado en `_normalize_fields()` |
| `estado` | `estado` | pasado tal cual a silver |

La normalización ocurre en `validate_gps/handler.py::_normalize_fields()` — en la frontera de ingestión, antes de cualquier validación. El storage en silver y las queries Athena usan los nombres internos.

### CSV de mantenimiento (batch)

| Campo CSV (PDF) | Campo interno | Transformación |
|---|---|---|
| `equipo_id` | `equipo_id` | uppercase |
| `fecha` | `fecha_mantenimiento` | renombrado + parse ISO |
| `tipo_falla` | `tipo_falla` | texto libre, sin enum |
| `criticidad` | `criticidad` | uppercase; validado contra `{ALTA, MEDIA, BAJA}` |

El SQL usa `WHERE criticidad = 'ALTA'` para definir "falla crítica" (per PDF: `criticidad` es la severidad, `tipo_falla` es la descripción).

---

## Supuestos

1. Timestamps GPS en **UTC** — si no tienen timezone, se asume UTC.
2. **`criticidad = 'ALTA'`** = falla crítica per PDF.
3. Pérdida de señal = sin **evento GPS válido** por >10 min (evento inválido no actualiza el reloj).
4. CSV de mantenimiento llega **una vez al día** en S3 bronze.
5. Flota de **escala moderada** (<10k equipos) — `detect_signal_loss` usa Scan; para >10k migrar a GSI sobre `last_seen`.
6. Bounding box Áncash: lat ∈ [−10.5, −7.8], lon ∈ [−78.5, −76.5].
7. `equipo_id` en formato `CAM_NNN` per PDF (ej: `CAM_001`).

---

## Quick start (LocalStack)

```bash
cd gps-pipeline
cp .env.example .env

# Levanta LocalStack + crea todos los recursos AWS
docker compose up -d

# Verifica
docker compose logs infra-init
```

### Verificar recursos

```bash
export EP="--endpoint-url=http://localhost:4566"
aws $EP s3 ls                          # gps-bronze, gps-silver, gps-gold
aws $EP kinesis list-streams           # gps-eventos
aws $EP dynamodb list-tables           # gps-last-seen, gps-dedup
```

### Correr el producer GPS

```bash
source .venv/bin/activate   # o: python -m venv .venv && pip install -r requirements.txt

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
AWS_DEFAULT_REGION=us-east-1 AWS_ENDPOINT_URL=http://localhost:4566 \
KINESIS_STREAM_NAME=gps-eventos PYTHONPATH=src \
python3 -m producer.simulator
```

### Subir CSV de mantenimiento

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
AWS_DEFAULT_REGION=us-east-1 \
aws --endpoint-url=http://localhost:4566 s3 cp \
  data/mantenimiento_sample.csv \
  s3://gps-bronze/mantenimientos/mantenimiento_sample.csv
```

### Tests

```bash
source .venv/bin/activate
PYTHONPATH=src pytest tests/ -v         # 45 tests (unit + integration contra LocalStack)
PYTHONPATH=src pytest tests/ -v -k "not Integration"   # solo unit (sin LocalStack)
```

### Dashboard local

```bash
PYTHONPATH=src:src/lambdas streamlit run src/dashboard/app.py
# → http://localhost:8501
```

---

## Deploy a AWS real

```bash
# 1. Infraestructura
cd infra/terraform
terraform init
terraform apply -var="use_localstack=false"

# 2. Lambdas
AWS_ENDPOINT_URL="" bash infra/scripts/deploy_lambdas.sh
```

Ver [docs/bonus.md](docs/bonus.md) para costos estimados, CI/CD y evidencias de despliegue.

---

## Estructura

```
gps-pipeline/
├── src/
│   ├── producer/          # simulador GPS (campos PDF: latitud, longitud, velocidad)
│   ├── lambdas/
│   │   ├── common/        # logger JSON estructurado
│   │   ├── validate_gps/  # normaliza campos + valida + dedup + → silver
│   │   └── detect_signal_loss/  # DynamoDB scan → SNS
│   ├── batch/             # ingest_maintenance: CSV bronze → Parquet silver
│   ├── quality/           # Pandera schemas + checker + métricas → gold
│   └── dashboard/         # Streamlit (alternativa local a QuickSight)
├── sql/
│   ├── create_tables.sql              # DDL Athena + Glue Catalog
│   └── equipos_fallas_criticas.sql   # query principal (Parte 3)
├── infra/
│   ├── scripts/bootstrap.sh          # idempotente, corre al arrancar Docker
│   └── terraform/                    # main.tf + lambdas.tf + monitoring.tf + quicksight.tf
├── tests/                            # 45 tests: unit + integración LocalStack
├── data/mantenimiento_sample.csv     # formato PDF: equipo_id, fecha, tipo_falla, criticidad
├── docker-compose.yml                # LocalStack + infra-init + dashboard
├── docs/
│   ├── architecture.md   # diagrama Mermaid + explicación de flujos
│   ├── bonus.md          # costos, CI/CD, supuestos, evidencias
│   └── explaining.md     # guía de aprendizaje de cada componente
└── .github/workflows/ci.yml          # GitHub Actions: test → plan → deploy
```

---

## Stack

| Capa | Tecnología | Por qué |
|---|---|---|
| Streaming ingest | Kinesis Data Streams (2 shards) | orden por equipo, retención 24h, múltiples consumers |
| Validación + normalización | Lambda Python 3.12 | serverless, escala a 0 |
| Estado last-seen | DynamoDB on-demand | microsegundos por clave, sin servidor |
| Alertas | SNS | fan-out a email/SMS/Lambda sin código extra |
| Cola de errores | SQS DLQ | no perder registros fallidos |
| Storage | S3 medallion Parquet Snappy | 70% menos costo Athena vs CSV |
| Batch ETL | Lambda + Pandas | suficiente para CSVs diarios; Glue para volúmenes mayores |
| Calidad | Pandera | schema-as-code, lazy validation, failure_cases estructurado |
| Query | Athena | serverless SQL sobre S3, sin bases de datos que mantener |
| Dashboard real | QuickSight + SPICE | no mover datos, refresh programado |
| Dashboard local | Streamlit | cero costo, cero licencias, ideal para demo |
| Reproducibilidad | LocalStack 3.4 + Docker | un comando para levantar todo |
| IaC | Terraform | mismo código para local y producción |
