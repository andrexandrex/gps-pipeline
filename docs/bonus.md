# Bonus — Costos, CI/CD y Supuestos

## Supuestos explícitos

| Supuesto | Impacto en el diseño |
|---|---|
| Timestamps GPS están en **UTC** | `datetime.fromisoformat()` asume UTC si no hay timezone info |
| `criticidad = 'ALTA'` equivale a "falla crítica" per PDF | SQL filtra `WHERE criticidad = 'ALTA'` |
| Pérdida de señal = sin evento GPS válido por >10 min | `detect_signal_loss` usa `last_seen` en DynamoDB; un evento inválido no actualiza el reloj |
| CSV de mantenimiento llega **una vez por día** en S3 | Lambda batch; no se diseñó para streaming de CSVs |
| La flota es de **escala moderada** (<10k equipos) | DynamoDB Scan en `detect_signal_loss`; para >10k migrar a GSI sobre `last_seen` |
| `equipo_id` en formato `CAM_NNN` per PDF | Validación solo verifica no-nulo; no impone formato |
| Registro GPS es autoritativo: si el GPS dice `velocidad=0` y `estado=ACTIVO`, se guarda tal cual | No se cruza con fuentes externas |
| **Bounding box Áncash**: lat ∈ [−10.5, −7.8], lon ∈ [−78.5, −76.5] | Coordenadas fuera → bronze_rejected; se puede ampliar si hay equipos en región limítrofe |

---

## Bonus 1 — Optimización de costos

### S3 y almacenamiento

| Acción | Ahorro estimado | Cómo |
|---|---|---|
| **Parquet + Snappy** vs CSV | 60–80% menos storage y bytes leídos en Athena | Ya implementado en silver/ y gold/ |
| **S3 Lifecycle**: bronze → IA a los 30 días → Glacier a los 90 días | ~60% en costos de bronze | `aws_s3_bucket_lifecycle_configuration` en Terraform |
| **Partition projection** en Athena | Elimina `MSCK REPAIR TABLE`; Athena no scannea particiones vacías | Ya en `sql/create_tables.sql` |
| Eliminar archivos pequeños en silver/ (S3 compaction) | Reduce número de requests S3 y mejora velocidad Athena | Job nocturno Glue que hace coalesce |

```hcl
# S3 Lifecycle — añadir a main.tf
resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id = "archive-bronze"
    filter { prefix = "" }
    transition { days = 30;  storage_class = "STANDARD_IA" }
    transition { days = 90;  storage_class = "GLACIER" }
    status = "Enabled"
  }
}
```

### Kinesis

| Situación | Recomendación |
|---|---|
| <1 MB/s de eventos GPS | Usar **On-Demand mode** (paga por uso, sin capacidad reservada) |
| >1 MB/s sostenido | Provisioned con 2 shards como está; añadir shard si IteratorAge sube |
| Reducir costo de Lambda | Subir `batch_size` de 100 a 500 y `maximum_batching_window` de 10s a 30s → menos invocaciones, mismo throughput |

### Lambda

```
validate_gps     → 256 MB, 60s timeout   — OK para el volumen actual
detect_signal_loss → 128 MB, 60s timeout — Scan DynamoDB es liviano
ingest_maintenance → 512 MB, 120s timeout — pandas + pyarrow necesita memoria

Monitorear con Lambda Power Tuning Tool (open-source de AWS):
Puede que 192 MB sea suficiente para validate_gps → -25% costo por invocación
```

### Athena

| Acción | Impacto |
|---|---|
| Usar **columnas necesarias** en SELECT (evitar `SELECT *`) | Athena cobra por bytes leídos; Parquet solo lee columnas solicitadas |
| **QuickSight SPICE** con refresh diario (no directo a Athena) | Un refresh nocturno en vez de queries en tiempo real; -80% en costo Athena para dashboards |
| Reutilizar **Saved Queries** para el reporte periódico | Evita re-escanear la misma data |

### Costo mensual estimado (flota de 50 equipos, sin Free Tier)

| Servicio | Uso estimado | Costo USD/mes |
|---|---|---|
| Kinesis (2 shards on-demand) | 50 equipos × 2 eventos/min × 43k min | ~$2 |
| Lambda validate_gps | 50 × 2880 invocaciones/día × 30 días × 256MB × 100ms | ~$1 |
| S3 silver + gold (Parquet) | ~5 GB/mes | ~$0.12 |
| DynamoDB (on-demand) | ~50 writes/min | ~$0.50 |
| Athena | 1 query/hora × 30 días × 10 MB promedio | ~$0.05 |
| SNS | <1000 alerts/mes | $0 (Free Tier) |
| **Total** | | **~$4–6/mes** |

QuickSight añade $18/mes por autor (Enterprise) o $5/mes (Standard). La alternativa Streamlit cuesta $0.

---

## Bonus 2 — CI/CD

El pipeline está en `.github/workflows/ci.yml` y tiene 5 jobs:

```
Push/PR a main
    │
    ├─► [unit-tests]       pytest unit — sin AWS, <30s
    │        │
    │   (paralelo)
    │        ├─► [integration-tests]   LocalStack en GitHub Service Container
    │        ├─► [terraform-plan]      solo en PRs — valida sin aplicar
    │        └─► [package-lambdas]     crea build/*.zip → artifact
    │
    └─► [deploy]           solo en push a main + aprobación manual
             ├─ terraform apply -var="use_localstack=false"
             ├─ deploy_lambdas.sh (usa los zips del artifact)
             └─ smoke test: verifica que S3/Kinesis/DynamoDB existen
```

### Secrets necesarios en GitHub

```
Settings → Secrets → Actions:
  AWS_ACCESS_KEY_ID       → IAM user con permisos mínimos
  AWS_SECRET_ACCESS_KEY   → idem
```

### Variables por entorno (dev vs prod)

```yaml
# .github/environments/production.yml (GitHub Environments)
# Configurar en Settings → Environments → production:
# - Required reviewers: 1 (aprobación manual antes de deploy)
# - Deployment branches: main only
```

### IAM mínimo para CI/CD (no AdministratorAccess)

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:*",
    "kinesis:*",
    "lambda:*",
    "dynamodb:*",
    "sns:*",
    "sqs:*",
    "iam:GetRole", "iam:PassRole",
    "glue:*",
    "cloudwatch:PutMetricAlarm"
  ],
  "Resource": "arn:aws:*:us-east-1:ACCOUNT_ID:*gps*"
}
```

### Evidencia para la entrega

Después del deploy a AWS real, capturar:

```bash
# 1. Archivos en S3
aws s3 ls s3://gps-silver/gps_eventos/ --recursive | head -5

# 2. CloudWatch logs de la Lambda
aws logs get-log-events \
  --log-group-name /aws/lambda/validate_gps \
  --log-stream-name $(aws logs describe-log-streams \
    --log-group-name /aws/lambda/validate_gps \
    --order-by LastEventTime --descending \
    --query 'logStreams[0].logStreamName' --output text) \
  --limit 20

# 3. DynamoDB — filas de last_seen
aws dynamodb scan --table-name gps-last-seen --max-items 5

# 4. Athena — resultado del query de fallas críticas
aws athena start-query-execution \
  --query-string "$(cat sql/equipos_fallas_criticas.sql)" \
  --query-execution-context Database=gps_pipeline \
  --result-configuration OutputLocation=s3://gps-gold/athena-results/
```
