# Arquitectura GPS Pipeline

## Diagrama

```mermaid
flowchart TD
    subgraph SOURCES["Fuentes de datos"]
        SIM["🚛 Simulador GPS\n(producer/simulator.py)\nEquipos cada 30s"]
        CSV["📄 CSV Mantenimiento\nUpload manual / SFTP"]
    end

    subgraph STREAMING["Camino Streaming — tiempo real"]
        direction TB
        KDS["Amazon Kinesis\nData Streams\ngps-eventos\n2 shards"]
        VLAMBDA["λ validate_gps\nValidación coords,\ntimestamp, duplicados\n(equipo_id+ts)"]
        DLQ["SQS DLQ\ngps-validate-dlq\nregistros inválidos"]
        DYNAMO["DynamoDB\ngps-last-seen\nÚltimo timestamp\npor equipo_id"]
        SLAMBDA["λ detect_signal_loss\nEventBridge cada 5 min\nCompara now() vs last_seen"]
        SNS["SNS\ngps-alertas\nAlerta pérdida >10 min"]
        BRONZE_GPS["S3 Bronze\ngps-bronze/bronze_rejected/\nregistros inválidos\n(NDJSON)"]
    end

    subgraph BATCH["Camino Batch — diario"]
        direction TB
        BRONZE_MANT["S3 Bronze\ngps-bronze/mantenimientos/\nCSV raw"]
        GLUE["AWS Glue Job\n(o Lambda batch)\nParseo, normalización\ntipo_falla, fechas"]
        SILVER_MANT["S3 Silver\ngps-silver/mantenimientos/\nParquet limpio"]
    end

    subgraph QUALITY["Calidad de datos"]
        PANDERA["λ / Glue Quality\nPandera schemas\n% completitud\n% duplicados\n% fuera de rango"]
        GOLD["S3 Gold\ngps-gold/quality_metrics/\nMétricas DQ"]
    end

    subgraph ANALYTICS["Capa analítica"]
        SILVER_GPS["S3 Silver\ngps-silver/gps_eventos/\nParquet validado"]
        GLUE_CAT["AWS Glue Data Catalog\nTablas: gps_eventos\nmantenimientos"]
        ATHENA["Amazon Athena\nSQL — equipos con\n>3 fallas críticas\nestado OK / SIN_SEÑAL"]
        QS["Amazon QuickSight\nDashboard operacional"]
    end

    subgraph OBS["Observabilidad"]
        CW["CloudWatch\nAlarmas: Lambda errors\nIteratorAge > 60s\nDLQ depth > 0"]
    end

    %% Streaming flow
    SIM -->|"PutRecord boto3\n(latitud/longitud/velocidad)"| KDS
    KDS -->|"Event source mapping"| VLAMBDA
    VLAMBDA -->|"Válido → Parquet directo"| SILVER_GPS
    VLAMBDA -->|"Inválido → NDJSON"| BRONZE_GPS
    VLAMBDA -->|"Inválido → DLQ"| DLQ
    VLAMBDA -->|"UpdateItem last_seen"| DYNAMO
    SILVER_GPS --> GLUE_CAT

    %% Signal loss detection
    DYNAMO -->|"Scan equipos"| SLAMBDA
    SLAMBDA -->|"Publish alert"| SNS

    %% Batch flow
    CSV -->|"s3 cp / evento PutObject"| BRONZE_MANT
    BRONZE_MANT -->|"EventBridge S3 trigger"| GLUE
    GLUE --> SILVER_MANT
    SILVER_MANT --> GLUE_CAT

    %% Quality
    SILVER_GPS --> PANDERA
    SILVER_MANT --> PANDERA
    PANDERA --> GOLD

    %% Analytics
    GLUE_CAT --> ATHENA
    ATHENA --> QS

    %% Observability
    VLAMBDA -.->|"métricas"| CW
    SLAMBDA -.->|"métricas"| CW
    KDS -.->|"IteratorAge"| CW

    %% Styles
    classDef lambda fill:#FF9900,color:#000,stroke:#c47000
    classDef storage fill:#3F8624,color:#fff,stroke:#2d6318
    classDef streaming fill:#8C4FFF,color:#fff,stroke:#6a3acc
    classDef analytics fill:#1A73E8,color:#fff,stroke:#1558b0
    classDef alert fill:#DD344C,color:#fff,stroke:#b02039

    class VLAMBDA,SLAMBDA,PANDERA lambda
    class BRONZE_GPS,BRONZE_MANT,SILVER_GPS,SILVER_MANT,GOLD storage
    class KDS,DLQ streaming
    class GLUE_CAT,ATHENA,QS analytics
    class SNS,CW alert
```

---

## Flujo streaming (tiempo real)

El simulador GPS (`src/producer/simulator.py`) publica eventos JSON cada ~30 segundos por equipo hacia **Kinesis Data Streams** (`gps-eventos`, 2 shards), usando `equipo_id` como partition key para garantizar orden por dispositivo. Kinesis activa la Lambda `validate_gps` mediante un *event source mapping* con batch size configurable (recomendado: 100 registros, ventana 10 s): esta función valida coordenadas (latitud −90/90, longitud −180/180), rechaza timestamps futuros o con antigüedad >1 hora, y detecta duplicados consultando DynamoDB por la clave `equipo_id + timestamp`. Los registros válidos se escriben en S3 Bronze vía Kinesis Firehose (Parquet, compresión Snappy, particiones `yyyy/mm/dd/hh`) y se actualiza el campo `last_seen` en DynamoDB. Los registros inválidos se envían a la SQS DLQ para auditoría sin perder el mensaje. Una segunda Lambda, `detect_signal_loss`, se ejecuta cada 5 minutos a través de **EventBridge Scheduler**; escanea la tabla DynamoDB, calcula `now() − last_seen` por equipo, y publica una alerta en **SNS** (`gps-alertas`) por cada equipo que supere los 10 minutos sin señal. CloudWatch monitorea errores de Lambda, profundidad de DLQ e `IteratorAge` del stream (métrica crítica: si sube indica que el consumidor no está al día).

## Flujo batch (diario)

Los archivos CSV de mantenimiento se depositan en `s3://gps-bronze/mantenimientos/` (carga manual, SFTP, o proceso externo). Un evento **S3 PutObject** dispara automáticamente el job de **AWS Glue** (o una Lambda para volúmenes pequeños, ver nota de diseño abajo), que lee el CSV raw, normaliza los campos (`tipo_falla` → enum `CRITICA/MENOR`, fechas ISO-8601, `equipo_id` en mayúsculas), y escribe Parquet particionado por `fecha_mantenimiento` en `s3://gps-silver/mantenimientos/`. El **Glue Data Catalog** mantiene las definiciones de tabla para ambas capas silver (`gps_eventos` y `mantenimientos`), lo que permite a **Athena** consultarlas con SQL estándar sin mover datos. La capa gold recibe las métricas de calidad calculadas por el módulo Pandera: porcentaje de completitud, duplicados y valores fuera de rango, escritas en JSON/Parquet para trazabilidad. **QuickSight** se conecta directamente a Athena para el dashboard operacional.

---

> **Nota de diseño — Glue vs Lambda para batch:**
> Se elige Glue porque escala horizontalmente con DPUs y tiene integración nativa con el Catalog. La alternativa Lambda+Pandas es más barata para archivos <128 MB y latencia <15 min, pero tiene límite de memoria (10 GB) y no tiene checkpointing automático. Para este caso con CSVs diarios de tamaño moderado, ambas son válidas; Glue se defiende mejor ante crecimiento de volumen.
