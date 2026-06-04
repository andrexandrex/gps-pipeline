# GPS Pipeline — Guía de aprendizaje

> Este documento explica cada pieza del proyecto: qué es, por qué existe aquí,
> qué es esencial vs qué es un plus, y los detalles de implementación que no son
> obvios. Se actualiza con cada nueva tarea.

---

## Índice

1. [El problema que resuelve este proyecto](#1-el-problema)
2. [Cómo correr el proyecto (comandos correctos)](#2-cómo-correr)
3. [Contenedores y Docker](#3-contenedores-y-docker)
4. [LocalStack — AWS falso en tu laptop](#4-localstack)
5. [Variables de entorno y secretos](#5-variables-de-entorno)
6. [Terraform — infraestructura como código](#6-terraform)
7. [Bootstrap — el script de arranque](#7-bootstrap)
8. [Los dos requirements.txt](#8-requirements)
9. [Medallion Architecture — bronze, silver, gold](#9-medallion)
10. [AWS Kinesis — la tubería de datos en tiempo real](#10-kinesis)
11. [AWS Lambda — funciones sin servidor](#11-lambda)
12. [AWS DynamoDB — la base de datos NoSQL](#12-dynamodb)
13. [AWS SNS y SQS — alertas y colas](#13-sns-sqs)
14. [Python: boto3 — el SDK para hablar con AWS](#14-boto3)
15. [Python: os.getenv — leer configuración sin hardcodear](#15-getenv)
16. [El logger JSON estructurado (y por qué frozenset)](#16-logger)
17. [Clientes lazy (\_dynamo, \_s3 globales)](#17-lazy-clients)
18. [Lambda validate_gps — decisiones de diseño](#18-validate-gps)
19. [Deduplicación con DynamoDB conditional write](#19-dedup)
20. [Lambda detect_signal_loss — diseño](#20-signal-loss)
21. [Tests: unitarios vs integración](#21-tests)

---

## 1. El problema

Tienes una flota de equipos pesados (excavadoras, volquetes) en Áncash, Perú.
Cada equipo envía su posición GPS cada 30 segundos. Además, los técnicos registran
mantenimientos en un CSV que se carga diariamente.

El proyecto resuelve dos preguntas:
- **Tiempo real**: ¿algún equipo dejó de enviar señal? (puede estar averiado o robado)
- **Análisis histórico**: ¿qué equipos tienen más fallas críticas? ¿cuándo fue su último GPS?

Hay dos "caminos" de datos:

```
GPS cada 30s  →  Kinesis  →  Lambda validate  →  S3 silver  →  Athena
CSV diario    →  S3 raw   →  Glue job         →  S3 silver  →  Athena
```

---

## 2. Cómo correr

**Error frecuente**: correr comandos desde el directorio equivocado.

```bash
# CORRECTO — primero entra al proyecto
cd ~/code_AWS/gps-pipeline

# CORRECTO — usa python3 (no python) porque el entorno conda tiene python3
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ENDPOINT_URL=http://localhost:4566 \
KINESIS_STREAM_NAME=gps-eventos \
PYTHONPATH=src \
python3 -m producer.simulator

# INCORRECTO — no funciona desde el directorio padre
cd ~/code_AWS
PYTHONPATH=src python -m producer.simulator   # ← falla: no encuentra el módulo
```

**Por qué `PYTHONPATH=src`**: Python busca módulos en los directorios de `sys.path`.
Al decir `PYTHONPATH=src`, le dices "busca también en la carpeta `src/`". Sin eso,
`import boto3` funciona, pero `from lambdas.common.logger import get_logger` no,
porque Python no sabe que `lambdas/` vive dentro de `src/`.

**Por qué `python3` y no `python`**: en tu entorno conda, `python` puede apuntar
a una versión diferente que no tiene las dependencias instaladas. `python3` apunta
al sistema donde sí están.

---

## 3. Contenedores y Docker

Un **contenedor** es como una caja sellada que incluye el programa + todas sus
dependencias + la configuración. No importa si tu laptop tiene Ubuntu, Mac o Windows:
la caja siempre se comporta igual.

**`docker-compose.yml`** es el archivo que define qué cajas levantar y cómo
conectarlas. En este proyecto levanta dos servicios:

```
localstack   ←  la caja que simula AWS
infra-init   ←  la caja que crea los recursos dentro de localstack al arrancar
```

**Comando clave**:
```bash
docker compose up -d    # levanta todo en segundo plano (-d = detached)
docker compose down     # apaga todo
docker logs gps-localstack   # ver qué está pasando dentro
```

**Por qué contenedores y no instalar todo directo**: si instalas LocalStack directo
en tu laptop, puede entrar en conflicto con otras versiones de Python o librerías.
Con Docker, cada proyecto tiene su propio entorno aislado. Cualquier persona puede
levantar esto con un solo comando sin instalar nada más que Docker.

---

## 4. LocalStack

LocalStack es un programa que **emula los servicios de AWS dentro de Docker**.
Cuando tu código hace `boto3.client("kinesis").put_record(...)`, en vez de ir
a los servidores de Amazon en us-east-1, va a `http://localhost:4566` — tu propia
laptop.

**Ventajas**:
- Sin cuenta AWS → sin costo
- Sin latencia de red real
- Puedes destruir y recrear todo en segundos
- Reproducible: cualquier colega puede levantar el mismo entorno

**La URL mágica**: `http://localhost:4566` es donde escucha LocalStack.
Cuando una Lambda corre *dentro* del contenedor Docker, esa URL cambia a
`http://localstack:4566` (el nombre del servicio en la red Docker interna).
Por eso en el `docker-compose.yml` ves `AWS_ENDPOINT_URL=http://localstack:4566`
para las Lambdas, y en tu terminal local usas `http://localhost:4566`.

**Servicios emulados en este proyecto**:
```
s3        → almacenamiento de archivos (los buckets bronze/silver/gold)
kinesis   → la tubería de streaming GPS
lambda    → las funciones de validación y detección
dynamodb  → base de datos para last_seen y dedup
sns       → envío de alertas
sqs       → cola para mensajes fallidos (DLQ)
glue      → catálogo de tablas para Athena
events    → EventBridge (disparar Lambda cada 5 min)
```

---

## 5. Variables de entorno

### El problema de hardcodear

```python
# MAL — nunca hagas esto
dynamodb.put_item(TableName="gps-last-seen", ...)  # ¿y si en prod se llama diferente?
endpoint = "http://localhost:4566"  # ¿y cuando lo subes a AWS real?
```

Si hardcodeas, tienes que cambiar el código para cambiar de entorno. Eso es frágil
y peligroso (puedes subir credenciales reales a GitHub).

### La solución: variables de entorno + `.env`

```python
# BIEN — lee del entorno
table_name = os.getenv("DYNAMO_TABLE_NAME", "gps-last-seen")  # "gps-last-seen" es el default
endpoint   = os.getenv("AWS_ENDPOINT_URL")  # None en producción real, URL en local
```

**`.env.example`** es el archivo que está en el repo. Contiene los nombres de las
variables pero con valores de ejemplo. Es seguro subirlo a GitHub.

**`.env`** es el archivo real con tus valores. Está en `.gitignore` — **nunca** se
sube al repo. Lo creas tú copiando `.env.example`:
```bash
cp .env.example .env
# editas .env con tus valores reales si fuera necesario
```

### Por qué las credenciales en docker-compose y no solo en .env

En `docker-compose.yml` defines las variables para los **contenedores**. En `.env`
defines las variables para tu **terminal local**. Son dos contextos distintos:

```yaml
# docker-compose.yml — para los contenedores
environment:
  - AWS_ACCESS_KEY_ID=test       # LocalStack acepta cualquier valor
  - AWS_ENDPOINT_URL=http://localstack:4566
```

```bash
# .env / terminal — para tus scripts Python locales
AWS_ACCESS_KEY_ID=test
AWS_ENDPOINT_URL=http://localhost:4566   # ← diferente! localhost, no localstack
```

**Regla de oro**: en LocalStack las credenciales pueden ser `test`/`test` — no son
reales. En AWS real, NUNCA pongas credenciales en ningún archivo del repo.

---

## 6. Terraform

**Terraform** es una herramienta que te permite describir la infraestructura cloud
como código (texto), y luego crear/modificar/destruir esa infraestructura con un comando.

### Sin Terraform (problema)
```
1. Entras a la consola de AWS
2. Clickeas "Crear bucket S3"
3. Escribes el nombre, seleccionas opciones
4. Repites para Kinesis, DynamoDB, Lambda...
5. Seis meses después nadie sabe qué configuración tenías
6. Tu colega no puede reproducirlo
```

### Con Terraform (solución)
```hcl
# infra/terraform/main.tf — describes el estado deseado
resource "aws_s3_bucket" "bronze" {
  bucket = "gps-bronze"
}
```
```bash
terraform apply   # crea el bucket exactamente así
terraform destroy # lo borra
```

### La variable `use_localstack`

```hcl
variable "use_localstack" {
  type    = bool
  default = true
}
```

Cuando es `true`, Terraform apunta al `endpoint_url` de LocalStack.
Cuando es `false` (en producción), apunta a AWS real. El **mismo código**,
dos comportamientos. Lo cambias con:
```bash
terraform apply -var="use_localstack=false"  # producción real
```

### Terraform vs bootstrap.sh — ¿cuál usar?

| | `bootstrap.sh` | Terraform |
|---|---|---|
| Velocidad | Rápido, sin instalación extra | Requiere `terraform init` |
| Estado | Sin estado (crea o ignora si existe) | Trackea estado en `.tfstate` |
| Ideal para | Dev local rápido, CI simple | Producción, equipos, cambios controlados |

En este proyecto usamos **ambos**: el `bootstrap.sh` para el arranque automático
en Docker, y Terraform cuando queremos gestionar infra real con control de cambios.

---

## 7. Bootstrap — el script de arranque

`infra/scripts/bootstrap.sh` corre automáticamente cuando LocalStack está listo
(lo ejecuta el contenedor `infra-init` en docker-compose).

**Su trabajo**: crear todos los recursos AWS necesarios la primera vez.

**Idempotencia**: si lo corres dos veces, no explota. Cada comando tiene
`2>/dev/null || echo "ya existe"`. Esto significa: "si el comando falla
(porque el recurso ya existe), ignora el error y sigue".

```bash
aws s3api create-bucket --bucket gps-bronze ...  \
  2>/dev/null || echo "  gps-bronze already exists, skipping"
# 2>/dev/null  → manda el error al "agujero negro" (lo ignora)
# || echo      → si falló, imprime el mensaje y continúa (no aborta)
```

**`set -euo pipefail`** al inicio: si cualquier comando falla *sin* ese `|| ...`,
el script se aborta inmediatamente. Es seguridad para no continuar con un estado
a medias.

---

## 8. Los dos requirements.txt

```
requirements.txt       ← lo que necesita el código EN PRODUCCIÓN
requirements-dev.txt   ← lo que necesita el código PARA DESARROLLAR/TESTEAR
```

`requirements-dev.txt` empieza con `-r requirements.txt`, lo que significa
"incluye todo lo de requirements.txt, más lo siguiente":

```
-r requirements.txt
pytest==8.2.2       ← para correr tests
pytest-mock==3.14.0 ← para hacer mocks en tests
moto==5.0.12        ← para simular AWS en tests unitarios sin necesitar LocalStack
```

**Por qué separados**: cuando empaquetas una Lambda para subir a AWS, no quieres
incluir `pytest` y `moto` — son 50 MB extra que hacen más lento el despliegue y
nunca se usan en producción. La Lambda solo necesita `requirements.txt`.

**Para desarrollo local instala el de dev**:
```bash
pip3 install -r requirements-dev.txt --break-system-packages
```

---

## 9. Medallion Architecture — bronze, silver, gold

Es una forma de organizar datos en capas, donde cada capa es más "limpia" que la anterior.

```
S3 gps-bronze/    ← datos RAW tal como llegan, sin tocar
S3 gps-silver/    ← datos validados y normalizados (Parquet)
S3 gps-gold/      ← métricas y agregados listos para consumir
```

### Por qué tres capas y no una

**Bronze (raw)**: guardas exactamente lo que llegó, con todos sus defectos.
Si tu validación tiene un bug, puedes volver a procesar desde aquí.
Es tu "cinta de seguridad". Nunca borras bronze.

**Silver (clean)**: datos validados, tipados, deduplicados. Formato Parquet
(columnar, comprimido). Aquí viven las tablas que consulta Athena.

**Gold (aggregated)**: resultados finales — métricas de calidad, reportes,
datos listos para QuickSight. Minimiza el costo de queries porque ya están
pre-calculados.

**bronze_rejected/**: una zona especial dentro de bronze para los registros
que fallaron validación. No se borran — sirven para auditoría y debug.

### Por qué Parquet y no CSV

| | CSV | Parquet |
|---|---|---|
| Tamaño | 100 MB | ~15 MB (comprimido) |
| Velocidad de query | Lee todo el archivo | Lee solo las columnas que necesita |
| Tipos de datos | Todo es string | Tipos reales (int, float, datetime) |
| Costo en Athena | Paga por bytes leídos | Mucho menos bytes leídos |

---

## 10. AWS Kinesis — la tubería streaming

Kinesis Data Streams es como una **tubería con memoria** para datos en tiempo real.

```
Producer (GPS)  →  [Kinesis Stream]  →  Consumer (Lambda)
                      ↑
                  guarda los datos
                  por 24h (default)
                  aunque el consumer falle
```

**Conceptos clave**:
- **Stream**: el canal. En este proyecto: `gps-eventos`.
- **Shard**: cada stream se divide en shards (particiones). 1 shard = 1 MB/s entrada.
  Usamos 2 shards para poder procesar en paralelo.
- **Partition key**: determina a qué shard va cada mensaje. Usamos `equipo_id`
  para que todos los mensajes del mismo equipo vayan al mismo shard → orden garantizado.
- **IteratorAge**: cuánto tiempo llevan los mensajes esperando ser procesados.
  Si sube mucho (>60s), el consumidor está atrasado → alarma CloudWatch.

**Por qué Kinesis y no SQS directo**: Kinesis retiene los mensajes hasta 24h
y permite múltiples consumers independientes. SQS borra el mensaje en cuanto
alguien lo lee. Kinesis es mejor para streams de alta frecuencia.

---

## 11. AWS Lambda — funciones sin servidor

Una Lambda es **una función Python que corre en la nube sin que tengas un servidor**.
AWS se encarga de:
- Proveer el servidor
- Escalarlo si llegan 1000 eventos a la vez
- Apagarlo cuando no hay trabajo

```python
def handler(event, context):
    # event = el dato que disparó la Lambda (un batch de Kinesis)
    # context = metadata de la invocación (tiempo restante, request ID)
    return {"valid": 5, "rejected": 1}
```

**Ciclo de vida (importante para entender los "lazy clients")**:
1. AWS crea un contenedor (arranque en frío, ~500ms)
2. Tu función corre
3. El contenedor queda "congelado" esperando más eventos (no se borra de inmediato)
4. Si llega otro evento pronto, usa el mismo contenedor (arranque en caliente, ~5ms)
5. Si no llega nada, AWS eventualmente destruye el contenedor

**Límites que afectan el diseño**:
- Tiempo máximo: 15 minutos (usamos 60 segundos)
- Memoria máxima: 10 GB (usamos 256 MB)
- Tamaño del paquete (zip): 250 MB descomprimido

**Triggers**: lo que dispara una Lambda. En este proyecto:
- `validate_gps` ← disparada por Kinesis (cada batch de eventos GPS)
- `detect_signal_loss` ← disparada por EventBridge (cada 5 minutos, como un cron)

---

## 12. AWS DynamoDB — base de datos NoSQL

DynamoDB es una base de datos **key-value** (clave-valor) de AWS. Muy rápida,
sin servidor, escala automáticamente.

**Por qué NoSQL y no una base SQL**: las consultas son siempre por clave primaria
(`equipo_id`). No necesitamos JOINs ni queries complejas. DynamoDB es microsegundos
para leer/escribir por clave. Una base SQL (RDS) costaría ~$20/mes aunque no la uses;
DynamoDB en `PAY_PER_REQUEST` cobra exactamente por lo que usas.

**Las dos tablas en este proyecto**:

```
gps-last-seen
  equipo_id (PK)  →  "EQ001"
  last_seen       →  "2024-01-15T10:30:00+00:00"

  Propósito: saber cuándo fue la última señal de cada equipo
  Usada por: validate_gps (escribe), detect_signal_loss (lee)

gps-dedup
  record_id (PK)  →  "EQ001#2024-01-15T10:30:00+00:00"
  ttl             →  1705345800  (Unix timestamp de expiración)

  Propósito: evitar procesar el mismo evento dos veces si Kinesis lo reintenta
  TTL: se auto-borra después de 24h — no necesita limpieza manual
```

**`PAY_PER_REQUEST`**: en vez de reservar capacidad fija, pagas por operación.
Perfecto para cargas variables. Si no hay eventos GPS de noche, no pagas nada.

---

## 13. AWS SNS y SQS — alertas y colas

### SNS (Simple Notification Service) — pub/sub de alertas

SNS es un sistema de **publicar → suscribir**. Publicas un mensaje una vez,
y llega a todos los suscriptores (email, SMS, otra Lambda, etc.).

En este proyecto: cuando `detect_signal_loss` detecta un equipo silencioso,
publica en el topic `gps-alertas`. En producción, suscribirías el email del
operador. En LocalStack, solo verificamos que el mensaje se publicó.

### SQS (Simple Queue Service) — cola de mensajes fallidos (DLQ)

**DLQ = Dead Letter Queue** (Cola de mensajes muertos). Es la "bandeja de entrada
para mensajes que nadie pudo procesar".

```
Kinesis → Lambda validate_gps
            ↓ falla 3 veces
          SQS gps-validate-dlq   ← el mensaje aterriza aquí
```

Sin DLQ, un mensaje que causa error se pierde silenciosamente o bloquea el stream.
Con DLQ, lo puedes inspeccionar después para entender qué falló.

**Configuración de reintentos en el Event Source Mapping**:
```
maxRetryAttempts = 2        → reintenta 2 veces antes de mandar a DLQ
bisectBatchOnFunctionError  → si falla el batch de 100, lo parte en dos mitades
                              y reintenta cada mitad por separado → aísla el registro malo
```

---

## 14. Python: boto3 — el SDK para hablar con AWS

`boto3` es la librería oficial de Python para usar servicios de AWS (o LocalStack).

```python
import boto3

# Crear un "cliente" para un servicio específico
kinesis = boto3.client("kinesis")  # en AWS real, usa credenciales del entorno
kinesis = boto3.client("kinesis", endpoint_url="http://localhost:4566")  # LocalStack

# Usar el cliente
response = kinesis.put_record(
    StreamName="gps-eventos",
    Data=json.dumps({"equipo_id": "EQ001", ...}).encode(),
    PartitionKey="EQ001",
)
```

**`boto3.client` vs `boto3.resource`**: `client` da acceso directo a la API REST
de AWS (más control, respuestas en diccionarios). `resource` es una abstracción
orientada a objetos (más cómodo, menos control). En este proyecto usamos `client`
porque necesitamos control exacto sobre las respuestas (e.g., detectar
`ConditionalCheckFailedException`).

**Credenciales**: boto3 las busca en este orden:
1. Parámetros directos en el código (nunca hagas esto)
2. Variables de entorno (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. Archivo `~/.aws/credentials`
4. IAM Role del servidor (en producción en EC2/Lambda)

En LocalStack usamos variables de entorno con valor `test` — LocalStack acepta cualquier valor.

---

## 15. Python: os.getenv — leer configuración sin hardcodear

```python
import os

# Lee la variable de entorno AWS_ENDPOINT_URL
# Si no existe, devuelve None
endpoint = os.getenv("AWS_ENDPOINT_URL")

# Con valor por defecto: si no existe, usa "gps-last-seen"
table = os.getenv("DYNAMO_TABLE_NAME", "gps-last-seen")
```

**El patrón en las funciones Lambda**:
```python
kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
client = boto3.client("dynamodb", **kw)
```

Esto dice: "si `AWS_ENDPOINT_URL` existe (LocalStack), úsalo; si no existe
(AWS real), no pases `endpoint_url` y boto3 usará los endpoints reales de AWS".
Un solo código, dos comportamientos.

`**kw` es "desempaquetar el diccionario como argumentos keyword":
```python
kw = {"endpoint_url": "http://localhost:4566"}
boto3.client("dynamodb", **kw)
# equivale a:
boto3.client("dynamodb", endpoint_url="http://localhost:4566")
```

---

## 16. El logger JSON estructurado (y por qué frozenset)

### Por qué logging estructurado (JSON) y no `print()`

```python
# MAL — texto libre, imposible de analizar con herramientas
print("Error procesando EQ001 a las 10:30")

# BIEN — JSON estructurado, CloudWatch puede filtrar y alertar sobre él
{"level": "ERROR", "equipo_id": "EQ001", "timestamp": "10:30", "message": "Error procesando"}
```

Con logs en JSON, CloudWatch Logs Insights puede hacer queries como:
```sql
filter level = "ERROR" and equipo_id = "EQ001"
| stats count() by bin(5m)
```

### La clase `_JsonFormatter`

```python
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "level": record.levelname,
            "message": record.getMessage(),
            ...
        }
        return json.dumps(entry)
```

`logging.Formatter` es la clase base de Python para formatear log records.
Al sobreescribir `format()`, controlamos exactamente qué aspecto tiene cada línea de log.

### El frozenset `_SKIP`

```python
_SKIP = frozenset(
    ("msg", "args", "levelname", "levelno", "pathname", "filename", ...)
)
```

Cuando Python crea un `LogRecord`, le añade ~20 campos internos (`levelname`,
`pathname`, `lineno`, etc.). Nosotros ya incluimos los que nos importan
(`level`, `message`, `timestamp`) de forma explícita. El loop final añade
los campos **extra** que el usuario agregó:

```python
logger.info("Record published", extra={"equipo_id": "EQ001", "shard": "shard-0"})
#                                       ↑ estos van a extra
```

El `frozenset` es la lista negra de campos internos que NO queremos duplicar.
`frozenset` en vez de `set` porque es inmutable — se crea una vez al definir la
clase y nunca cambia. Es más rápido para el operador `in` que una lista.

---

## 17. Clientes lazy (\_dynamo, \_s3 globales)

```python
_dynamo: Optional[boto3.client] = None   # empieza como None

def _dynamo_client() -> boto3.client:
    global _dynamo
    if _dynamo is None:
        kw = {"endpoint_url": ep} if (ep := os.getenv("AWS_ENDPOINT_URL")) else {}
        _dynamo = boto3.client("dynamodb", **kw)
    return _dynamo
```

**Por qué variables globales y no crear el cliente dentro de `handler()`**:

Cada vez que llamas `boto3.client(...)`, Python:
1. Lee las credenciales del entorno
2. Establece una conexión TCP con el endpoint
3. Crea el objeto cliente

Esto tarda ~100ms. Si la Lambda procesa 1000 eventos/minuto y recrea el cliente
cada vez, pierdes ~100ms por invocación innecesariamente.

Con el patrón lazy ("perezoso"):
- Primera invocación: crea el cliente y lo guarda en la variable global
- Invocaciones siguientes (arranque en caliente): reutiliza el cliente ya creado

`Optional[boto3.client]` es solo una anotación de tipo que dice "puede ser None
o un cliente boto3". No afecta el comportamiento en tiempo de ejecución.

---

## 18. Lambda validate_gps — decisiones de diseño

### Flujo completo por registro

```
[Kinesis batch de 100 eventos]
    ↓
for cada evento:
    1. base64 decode + JSON parse
    2. Validar (bbox, timestamp, campos requeridos)
    3. Si inválido → lista rejected
    4. Si válido → check dedup en DynamoDB
    5. Si duplicado → skip
    6. Si nuevo → actualizar last_seen + añadir a lista valid
    ↓
Escribir todos los valid → S3 silver (Parquet, un archivo por batch)
Escribir todos los rejected → S3 bronze_rejected (NDJSON, un archivo por batch)
```

### Por qué el raise al final del write de válidos

```python
try:
    _write_valid(valid)
except Exception as exc:
    logger.error("Failed to write valid batch to S3", ...)
    raise   # ← re-lanza la excepción
```

Si no podemos escribir los registros válidos a S3, **queremos que la Lambda falle**.
¿Por qué? Porque Kinesis retiene los mensajes 24h. Si la Lambda falla, Kinesis
reintentará con el mismo batch. El `bisectBatchOnFunctionError` dividirá el batch
para aislar el registro problemático. Si silenciamos el error (`pass` o `continue`),
los datos se pierden para siempre.

Para los rechazados, en cambio, no hacemos `raise`: perder un archivo de auditoría
es menos grave que perder datos válidos y forzar un retry que podría duplicar.

### El bbox de Áncash

```python
LAT_MIN, LAT_MAX = -10.5, -7.8   # sur a norte
LON_MIN, LON_MAX = -78.5, -76.5  # costa a sierra/selva
```

Esto es más estricto que solo validar `(-90, 90)` y `(-180, 180)`. Un GPS
con bug podría enviar coordenadas válidas globalmente pero absurdas para
un equipo en Áncash (ej: latitud 40°N = España). El bbox regional filtra esos casos.

---

## 19. Deduplicación con DynamoDB conditional write

**El problema**: Kinesis garantiza "al menos una vez" (at-least-once delivery).
Si una Lambda falla a mitad del batch, Kinesis reenvía todo el batch. Sin dedup,
procesaríamos algunos registros dos veces.

**La solución**: antes de procesar un registro, intentamos insertarlo en `gps-dedup`
con una condición:

```python
dynamo.put_item(
    TableName="gps-dedup",
    Item={"record_id": {"S": "EQ001#2024-01-15T10:30:00"}},
    ConditionExpression="attribute_not_exists(record_id)",  # ← la clave
)
```

`attribute_not_exists(record_id)` significa: "solo inserta si este `record_id`
NO existe todavía". Si ya existe, DynamoDB lanza `ConditionalCheckFailedException`.

```python
except ClientError as exc:
    if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
        return True   # es un duplicado, saltarlo
    raise   # otro error → propagar
```

**TTL (Time To Live)**: cada registro en `gps-dedup` incluye un campo `ttl`
con un Unix timestamp de "cuándo expirar". DynamoDB lo borra automáticamente
cuando ese tiempo pasa. Sin TTL, la tabla crecería indefinidamente.

```python
ttl = int((datetime.now(timezone.utc) + timedelta(hours=24)).timestamp())
# ejemplo: si ahora son las 10:00, ttl = timestamp de mañana a las 10:00
```

---

## 20. Lambda detect_signal_loss — diseño

Esta Lambda corre cada 5 minutos (EventBridge Scheduler) y hace:

```python
for cada equipo en DynamoDB:
    si now() - last_seen > 10 minutos:
        añadir a lista "perdidos"

si hay perdidos:
    publicar en SNS un solo mensaje con todos los equipos afectados
```

**Por qué un solo mensaje SNS y no uno por equipo**: si tienes 50 equipos
silenciosos a la vez (apagón general), no quieres 50 emails en segundos.
Un mensaje agrupado es más útil y menos ruidoso.

**Por qué Scan y no Query**: DynamoDB tiene dos formas de leer:
- `Query`: busca por clave conocida. Muy rápido.
- `Scan`: lee toda la tabla. Más lento, más costoso.

Usamos `Scan` porque no sabemos qué equipos van a estar silenciosos — tenemos
que revisar todos. Para flotas grandes (>10k equipos), la alternativa sería
crear un **GSI** (Global Secondary Index) en el campo `last_seen` y hacer un
`Query` buscando `last_seen < cutoff`. Eso sería O(resultados) en vez de O(tabla).

---

## 21. Tests: unitarios vs integración

### Tests unitarios (`test_validate_gps.py`)

Prueban **una función en aislamiento**, sin ningún servicio externo.

```python
def test_future_timestamp_rejected():
    rec = _good_record()
    rec["timestamp"] = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    ok, reason = _validate(rec)   # ← solo llama a la función de validación
    assert ok is False
```

**Ventajas**: rapidísimos (0.33s para 11 tests), no necesitan Docker ni red,
se pueden correr en CI sin setup.

```bash
PYTHONPATH=src pytest tests/test_validate_gps.py -v
```

### Tests de integración (`test_integration.py`)

Prueban **el flujo completo** contra LocalStack real.

```python
def test_valid_record_lands_in_silver(self, s3):
    result = handler(_kinesis_event([_good_record("EQ_IT_01")]), None)
    assert result["valid"] == 1
    objs = s3.list_objects_v2(Bucket="gps-silver", Prefix="gps_eventos/")
    assert objs.get("KeyCount", 0) >= 1
    # ↑ verifica que el archivo realmente está en S3
```

**Ventajas**: verifican que todo el sistema funciona junto (boto3 + DynamoDB + S3).
**Desventajas**: necesitan LocalStack corriendo, son más lentos (~1s vs 0.03s).

```bash
# Necesitas LocalStack corriendo: docker compose up -d
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
AWS_DEFAULT_REGION=us-east-1 AWS_ENDPOINT_URL=http://localhost:4566 \
PYTHONPATH=src pytest tests/test_integration.py -v
```

### `conftest.py` — configuración compartida de pytest

```python
# tests/conftest.py
sys.path.insert(0, os.path.abspath("src/lambdas"))
```

Esto agrega `src/lambdas` al path de Python **automáticamente para todos los tests**,
sin que cada archivo de test lo tenga que hacer. pytest carga `conftest.py` antes
de correr cualquier test.

---

*Este documento se actualiza con cada nueva tarea del proyecto.*
