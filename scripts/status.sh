#!/bin/bash
# STATUS CHECK — run this anytime to see what's happening in the pipeline
# Usage: bash scripts/status.sh

EP="--endpoint-url=http://localhost:4566"
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GPS Pipeline — Estado del sistema"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 1. LocalStack
echo ""
echo -e "${BLUE}[1] LocalStack (fake AWS)${NC}"
HEALTH=$(curl -sf http://localhost:4566/_localstack/health 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print('OK' if d.get('status')=='running' else 'FAIL')" 2>/dev/null || echo "DOWN")
if [ "$HEALTH" = "OK" ]; then
  echo -e "    Status: ${GREEN}● RUNNING${NC}  →  http://localhost:4566"
else
  echo -e "    Status: ${RED}● DOWN${NC}  —  corre: docker compose up -d"
fi

# 2. Kinesis stream
echo ""
echo -e "${BLUE}[2] Kinesis stream (gps-eventos)${NC}"
SHARDS=$(aws $EP kinesis describe-stream-summary --stream-name gps-eventos \
  --query StreamDescriptionSummary.OpenShardCount --output text 2>/dev/null || echo "0")
SEQ=$(aws $EP kinesis get-shard-iterator --stream-name gps-eventos --shard-id shardId-000000000000 \
  --shard-iterator-type LATEST --query ShardIterator --output text 2>/dev/null)
echo -e "    Shards activos: ${GREEN}$SHARDS${NC}"
echo "    Para leer eventos en vivo: ver sección [Comandos útiles] abajo"

# 3. DynamoDB — equipos rastreados
echo ""
echo -e "${BLUE}[3] DynamoDB — equipos rastreados (last-seen)${NC}"
EQUIPOS=$(aws $EP dynamodb scan --table-name gps-last-seen --select COUNT \
  --query Count --output text 2>/dev/null || echo "0")
echo -e "    Equipos con señal registrada: ${GREEN}$EQUIPOS${NC}"
if [ "$EQUIPOS" -gt "0" ] 2>/dev/null; then
  echo "    Últimos vistos:"
  aws $EP dynamodb scan --table-name gps-last-seen \
    --query 'Items[*].{ID:equipo_id.S, Seen:last_seen.S}' \
    --output table 2>/dev/null | head -15
fi

# 4. S3 — datos en cada capa
echo ""
echo -e "${BLUE}[4] S3 — datos en medallion${NC}"
for BUCKET in gps-bronze gps-silver gps-gold; do
  COUNT=$(aws $EP s3 ls s3://$BUCKET/ --recursive 2>/dev/null | grep -v '\.keep$' | wc -l)
  SIZE=$(aws $EP s3 ls s3://$BUCKET/ --recursive --human-readable 2>/dev/null | grep -v '\.keep$' | awk '{sum += $3} END {print sum+0 " KB"}')
  echo -e "    ${BUCKET}: ${GREEN}$COUNT archivos${NC}"
done
GPS_FILES=$(aws $EP s3 ls s3://gps-silver/gps_eventos/ --recursive 2>/dev/null | grep -v '\.keep$' | wc -l)
MANT_FILES=$(aws $EP s3 ls s3://gps-silver/mantenimientos/ --recursive 2>/dev/null | grep -v '\.keep$' | wc -l)
GOLD_FILES=$(aws $EP s3 ls s3://gps-gold/ --recursive 2>/dev/null | grep -v '\.keep$' | wc -l)
echo "    ├── silver/gps_eventos:    $GPS_FILES Parquets"
echo "    ├── silver/mantenimientos: $MANT_FILES Parquets"
echo "    └── gold/quality_metrics: $GOLD_FILES archivos"

# 5. DLQ — mensajes fallidos
echo ""
echo -e "${BLUE}[5] SQS DLQ — mensajes fallidos${NC}"
DLQ=$(aws $EP sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/gps-validate-dlq \
  --attribute-names ApproximateNumberOfMessages \
  --query Attributes.ApproximateNumberOfMessages --output text 2>/dev/null || echo "0")
if [ "$DLQ" = "0" ]; then
  echo -e "    Mensajes en DLQ: ${GREEN}0 ✓${NC}  (ningún evento falló)"
else
  echo -e "    Mensajes en DLQ: ${RED}$DLQ ⚠${NC}  (hay registros que fallaron — revisar bronze_rejected/)"
fi

# 6. Dashboard
echo ""
echo -e "${BLUE}[6] Streamlit Dashboard${NC}"
if curl -sf http://localhost:8501 >/dev/null 2>&1; then
  echo -e "    Status: ${GREEN}● RUNNING${NC}  →  http://localhost:8501  (abre en tu navegador)"
else
  echo -e "    Status: ${YELLOW}○ DETENIDO${NC}  —  para iniciarlo:"
  echo "    source .venv/bin/activate"
  echo "    PYTHONPATH=src:src/lambdas streamlit run src/dashboard/app.py"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${YELLOW}Comandos útiles:${NC}"
echo ""
echo "  Iniciar producer GPS (Terminal 1):"
echo "    cd ~/code_AWS/gps-pipeline"
echo "    source .venv/bin/activate"
echo "    PYTHONPATH=src:src/lambdas AWS_ACCESS_KEY_ID=test \\"
echo "    AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \\"
echo "    AWS_ENDPOINT_URL=http://localhost:4566 KINESIS_STREAM_NAME=gps-eventos \\"
echo "    python3 -m producer.simulator"
echo ""
echo "  Procesar batch CSV (una vez):"
echo "    source .venv/bin/activate"
echo "    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \\"
echo "    AWS_DEFAULT_REGION=us-east-1 \\"
echo "    aws --endpoint-url=http://localhost:4566 s3 cp \\"
echo "      data/mantenimiento_sample.csv \\"
echo "      s3://gps-bronze/mantenimientos/mantenimiento_$(date +%Y%m%d).csv"
echo "    PYTHONPATH=src:src/lambdas python3 scripts/run_batch.py"
echo ""
echo "  Ver logs del producer en tiempo real:"
echo "    (ejecuta el producer y mira la salida en la terminal)"
echo ""
echo "  Ver últimos GPS en silver:"
echo "    aws $EP s3 ls s3://gps-silver/gps_eventos/ --recursive | tail -5"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
