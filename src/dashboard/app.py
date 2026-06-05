"""
GPS Pipeline — Streamlit Dashboard
Reads from LocalStack S3 + DynamoDB. All computation happens in run_pipeline.py.

Start:
    env $(cat .env | grep -v '^#' | xargs) \\
    PYTHONPATH=src:src/lambdas \\
    streamlit run src/dashboard/app.py
"""

import io
import json
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

_ep   = os.getenv("AWS_ENDPOINT_URL")
_kw   = {"endpoint_url": _ep} if _ep else {}
SILVER     = os.getenv("SILVER_BUCKET",                 "gps-silver")
BRONZE     = os.getenv("BRONZE_BUCKET",                 "gps-bronze")
GOLD       = os.getenv("GOLD_BUCKET",                   "gps-gold")
DYNAMO     = os.getenv("DYNAMO_TABLE_NAME",             "gps-last-seen")
ALERT_MIN  = int(os.getenv("SIGNAL_LOSS_THRESHOLD_MINUTES",        "10"))
MAINT_MIN  = int(os.getenv("AUTO_MAINTENANCE_THRESHOLD_MINUTES",   "30"))


@st.cache_resource
def _s3():   return boto3.client("s3",   **_kw)
@st.cache_resource
def _sqs():  return boto3.client("sqs",  **_kw)
@st.cache_resource
def _dyn():  return boto3.client("dynamodb", **_kw)


# ── Loaders ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=15)
def load_equipment_status() -> pd.DataFrame:
    try:
        rows = []
        for page in _dyn().get_paginator("scan").paginate(TableName=DYNAMO):
            for item in page.get("Items", []):
                eid  = item.get("equipo_id", {}).get("S", "")
                seen = item.get("last_seen",  {}).get("S", "")
                if seen:
                    try:
                        dt   = datetime.fromisoformat(seen)
                        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                        mins = round((datetime.now(timezone.utc) - dt).total_seconds() / 60, 1)
                        if mins <= ALERT_MIN:          estado, icono = "OK",        "🟢"
                        elif mins <= MAINT_MIN:        estado, icono = "ALERTA",    "🟡"
                        else:                          estado, icono = "SIN SEÑAL", "🔴"
                    except ValueError:
                        mins, estado, icono = None, "DATO INVÁLIDO", "⚠️"
                else:
                    mins, estado, icono = None, "SIN REGISTRO", "⚠️"
                rows.append({"Equipo": eid,
                             "Última señal GPS": seen[:19].replace("T", " ") if seen else "—",
                             "Min sin señal":    mins,
                             "Estado":           f"{icono} {estado}"})
        df = pd.DataFrame(rows)
        return df.sort_values("Min sin señal", ascending=False, na_position="last") if not df.empty else df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_silver_parquet(prefix: str) -> pd.DataFrame:
    try:
        frames = []
        for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=SILVER, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    body = _s3().get_object(Bucket=SILVER, Key=obj["Key"])["Body"].read()
                    frames.append(pd.read_parquet(io.BytesIO(body)))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_quality_runs(dataset: str) -> pd.DataFrame:
    try:
        rows = []
        for obj in _s3().list_objects_v2(Bucket=GOLD, Prefix=f"quality_metrics/{dataset}/").get("Contents", []):
            if not obj["Key"].endswith(".json"): continue
            m = json.loads(_s3().get_object(Bucket=GOLD, Key=obj["Key"])["Body"].read())
            rows.append({
                "Fecha/hora":          m.get("run_timestamp", "")[:19].replace("T", " "),
                "Registros en silver": m.get("total_rows",       0),
                "% válidos":           round(m.get("valid_pct",        0), 1),
                "% duplicados":        round(m.get("duplicates_pct",   0), 1),
                "Errores Pandera":     m.get("pandera_failures",  0),
                "_completeness":       m.get("completeness_pct",  {}),
                "_out_of_range":       m.get("out_of_range_pct",  {}),
            })
        return pd.DataFrame(rows).sort_values("Fecha/hora") if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=15)
def load_robustez() -> dict:
    """DLQ depth, rejected record counts, and bronze_rejected file list."""
    result = {"dlq_visible": 0, "dlq_inflight": 0,
              "gps_rejected": 0, "mant_rejected": 0,
              "rejected_samples": []}
    try:
        ep_base = (_ep or "http://localhost:4566").rstrip("/")
        attrs = _sqs().get_queue_attributes(
            QueueUrl=f"{ep_base}/000000000000/gps-validate-dlq",
            AttributeNames=["ApproximateNumberOfMessages",
                            "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        result["dlq_visible"]  = int(attrs.get("ApproximateNumberOfMessages",           0))
        result["dlq_inflight"] = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    except Exception:
        pass

    try:
        result["gps_rejected"]  = _s3().list_objects_v2(
            Bucket=BRONZE, Prefix="bronze_rejected/gps_eventos/").get("KeyCount", 0)
        result["mant_rejected"] = _s3().list_objects_v2(
            Bucket=BRONZE, Prefix="bronze_rejected/mantenimientos/").get("KeyCount", 0)

        # Sample one rejected GPS file to show rejection reasons
        objs = _s3().list_objects_v2(
            Bucket=BRONZE, Prefix="bronze_rejected/gps_eventos/").get("Contents", [])
        if objs:
            latest = sorted(objs, key=lambda x: x["LastModified"])[-1]
            raw = _s3().get_object(Bucket=BRONZE, Key=latest["Key"])["Body"].read()
            result["rejected_samples"] = [
                json.loads(line) for line in raw.decode().splitlines() if line.strip()
            ][:5]
    except Exception:
        pass

    return result


# ── Page ──────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="GPS Pipeline — Áncash, Perú", page_icon="🚛", layout="wide")
st.title("🚛 GPS Pipeline — Áncash, Perú")

connected = f"🟢 LocalStack `{_ep}`" if _ep else "🔴 AWS real"
st.caption(
    f"Conexión: {connected}  |  Alerta GPS: **{ALERT_MIN} min**  |  "
    f"Auto-mantenimiento: **{MAINT_MIN} min**"
)

if st.button("🔄 Refrescar"):
    st.cache_data.clear()
    st.rerun()

tab_gps, tab_mant, tab_quality, tab_robustez = st.tabs([
    "📡 Estado GPS en tiempo real",
    "🔧 Análisis de Mantenimientos",
    "📊 Calidad de Datos",
    "⚙️ Robustez & Monitoreo",
])


# ═══════════════════════════════════════════════════════════
# TAB 1 — Estado GPS en tiempo real
# The ONLY tab that shows emergency (red) alerts.
# Red = equipo without GPS signal. That's the core business alert.
# ═══════════════════════════════════════════════════════════
with tab_gps:
    eq_df = load_equipment_status()

    if eq_df.empty:
        st.warning("Sin datos GPS. Corre: `make pipeline`")
    else:
        ok_df    = eq_df[eq_df["Estado"].str.contains("OK")]
        alert_df = eq_df[eq_df["Estado"].str.contains("ALERTA")]
        lost_df  = eq_df[eq_df["Estado"].str.contains("SIN SEÑAL")]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total equipos",   len(eq_df))
        c2.metric("🟢 Señal OK",     len(ok_df),
                  help=f"Señal recibida hace menos de {ALERT_MIN} min")
        c3.metric("🟡 Alerta",       len(alert_df),
                  help=f"Sin señal entre {ALERT_MIN} y {MAINT_MIN} min → alerta SNS enviada")
        c4.metric("🔴 Sin señal",    len(lost_df),
                  help=f"Sin señal más de {MAINT_MIN} min → SNS + registro mantenimiento automático")

        # ── ÚNICO banner de emergencia en el dashboard ──────────────────────
        if not lost_df.empty:
            st.error(
                f"🚨 **ALERTA OPERACIONAL — {len(lost_df)} equipo(s) sin señal GPS "
                f"por más de {ALERT_MIN} minutos.**\n\n"
                f"Acción automática ya ejecutada:\n"
                f"- **SNS** (`gps-alertas`): notificación enviada al operador\n"
                f"- **Silver/mantenimientos**: registro automático creado con "
                f"`tipo_falla='GPS DESCONECTADO'`, `criticidad='ALTA'`\n\n"
                + "Equipos: " + "  ".join(f"`{e}`" for e in lost_df["Equipo"].tolist())
            )
        elif not alert_df.empty:
            st.warning(
                f"⚠️ **{len(alert_df)} equipo(s) en alerta** (sin señal entre "
                f"{ALERT_MIN}–{MAINT_MIN} min). SNS notificado. "
                "Si persiste, se creará un registro de mantenimiento automático.\n\n"
                + "  ".join(f"`{e}`" for e in alert_df["Equipo"].tolist())
            )
        else:
            st.success(f"✅ Todos los equipos reportando señal GPS en los últimos {ALERT_MIN} min.")

        st.dataframe(eq_df, use_container_width=True,
                     column_config={"Min sin señal": st.column_config.NumberColumn(format="%.1f min")},
                     hide_index=True)

        chart_df = eq_df.dropna(subset=["Min sin señal"]).set_index("Equipo")[["Min sin señal"]]
        if not chart_df.empty:
            st.subheader("Tiempo sin señal por equipo")
            st.bar_chart(chart_df)
            st.caption(
                f"Umbral alerta SNS: {ALERT_MIN} min  |  "
                f"Umbral auto-mantenimiento: {MAINT_MIN} min"
            )


# ═══════════════════════════════════════════════════════════
# TAB 2 — Análisis de Mantenimientos (resultado query Athena)
# This is HISTORICAL analysis, not an emergency.
# Styling: informational, not red alert.
# ═══════════════════════════════════════════════════════════
with tab_mant:
    st.caption(
        "Datos de silver/mantenimientos/ — replica el resultado del query Athena "
        "(`sql/equipos_fallas_criticas.sql`). Son registros históricos, no alertas en tiempo real."
    )

    mant_df = load_silver_parquet("mantenimientos/")

    if mant_df.empty:
        st.info("Sin datos. Corre: `make pipeline`")
    else:
        is_auto  = mant_df.get("tipo_falla", pd.Series(dtype=str)).str.contains("GPS DESCONECTADO", na=False)
        manual_df = mant_df[~is_auto]
        auto_df   = mant_df[is_auto]
        alta_df   = mant_df[mant_df.get("criticidad", pd.Series(dtype=str)) == "ALTA"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total registros",    len(mant_df))
        c2.metric("Criticidad ALTA",    len(alta_df))
        c3.metric("Registros manuales (CSV)", len(manual_df))
        c4.metric("Auto-generados (GPS loss)", len(auto_df))

        # ── Fallas por equipo — informational, not emergency ────────────────
        if "criticidad" in mant_df.columns and "equipo_id" in mant_df.columns:
            st.subheader("Fallas de criticidad ALTA por equipo")
            fallas_df = (
                mant_df[mant_df["criticidad"] == "ALTA"]
                .groupby("equipo_id").size()
                .reset_index(name="Fallas ALTA")
                .sort_values("Fallas ALTA", ascending=False)
            )
            if not fallas_df.empty:
                st.bar_chart(fallas_df.set_index("equipo_id"))

                at_risk = fallas_df[fallas_df["Fallas ALTA"] > 3]
                if not at_risk.empty:
                    # Informational callout — NOT a red emergency banner
                    st.info(
                        f"📋 **Resultado del query Athena** — "
                        f"{len(at_risk)} equipo(s) con más de 3 fallas ALTA "
                        f"(candidatos prioritarios para mantenimiento preventivo):  \n"
                        + "  ".join(
                            f"`{row['equipo_id']}` — {row['Fallas ALTA']} fallas"
                            for _, row in at_risk.iterrows()
                        )
                    )
                    st.caption(
                        "Este análisis corresponde a la Parte 3 de la evaluación. "
                        "El query completo está en `sql/equipos_fallas_criticas.sql`. "
                        "Combina esta tabla con la señal GPS más reciente para "
                        "determinar el estado OK / SIN_SEÑAL de cada equipo."
                    )

        col_left, col_right = st.columns(2)
        with col_left:
            with st.expander(f"📄 {len(manual_df)} registros manuales (del CSV)", expanded=True):
                if not manual_df.empty:
                    cols = [c for c in ["equipo_id", "fecha_mantenimiento", "tipo_falla", "criticidad"]
                            if c in manual_df.columns]
                    st.dataframe(manual_df[cols], use_container_width=True, hide_index=True)
        with col_right:
            with st.expander(f"🤖 {len(auto_df)} registros auto-generados (GPS loss)"):
                if not auto_df.empty:
                    cols = [c for c in ["equipo_id", "fecha_mantenimiento", "tipo_falla", "criticidad"]
                            if c in auto_df.columns]
                    st.dataframe(auto_df[cols], use_container_width=True, hide_index=True)
                    st.caption(
                        f"Creados automáticamente por `detect_signal_loss` "
                        f"cuando un equipo supera {MAINT_MIN} min sin señal GPS."
                    )


# ═══════════════════════════════════════════════════════════
# TAB 3 — Calidad de Datos (Pandera)
# ═══════════════════════════════════════════════════════════
with tab_quality:
    dataset_sel = st.radio("Dataset", ["gps_eventos", "mantenimientos"], horizontal=True)
    runs_df = load_quality_runs(dataset_sel)

    if runs_df.empty:
        st.info("Sin métricas. Corre: `make pipeline`")
    else:
        latest = runs_df.iloc[-1]

        st.subheader("Último check de calidad")
        st.caption(f"Ejecutado: {latest['Fecha/hora']}")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registros analizados", int(latest["Registros en silver"]),
                  help="Filas totales en silver/ al momento del check")
        c2.metric("% Válidos",            f"{latest['% válidos']:.1f}%",
                  help="Filas que pasaron todas las reglas Pandera")
        c3.metric("% Duplicados",         f"{latest['% duplicados']:.1f}%",
                  help="Filas con clave duplicada (equipo_id + timestamp)")
        c4.metric("Errores Pandera",      int(latest["Errores Pandera"]),
                  help="Celdas que violan una regla del schema")

        if len(runs_df) > 1:
            st.subheader("Tendencia")
            st.line_chart(runs_df.set_index("Fecha/hora")[["% válidos", "% duplicados"]],
                          color=["#2ecc71", "#e74c3c"])

        comp = latest.get("_completeness", {})
        oor  = latest.get("_out_of_range",  {})
        if comp:
            st.subheader("Detalle por columna")
            col_df = pd.DataFrame([{
                "Columna":             c,
                "% completo":          f"{p:.1f}%",
                "% fuera de rango":    f"{oor.get(c, 0):.1f}%",
                "Estado":              "✅ OK" if p == 100 and oor.get(c, 0) == 0
                                        else ("⚠️ revisar" if p >= 95 else "🚨 crítico"),
            } for c, p in comp.items()])
            st.dataframe(col_df, use_container_width=True, hide_index=True)

        with st.expander("Historial de runs"):
            st.dataframe(runs_df[["Fecha/hora", "Registros en silver",
                                  "% válidos", "% duplicados", "Errores Pandera"]],
                         use_container_width=True, hide_index=True)
            st.caption(
                "'Registros en silver' crece con cada run porque el checker escanea "
                "todos los Parquet históricos — mide la calidad acumulada, no solo el último lote."
            )


# ═══════════════════════════════════════════════════════════
# TAB 4 — Robustez & Monitoreo
# Covers PDF Part 2 requirements:
# manejo de errores, logs, retry, monitoreo
# ═══════════════════════════════════════════════════════════
with tab_robustez:
    st.subheader("Estado operacional del pipeline")
    rob = load_robustez()

    # ── DLQ (Dead Letter Queue) ──────────────────────────────────────────────
    st.markdown("#### 🪣 Dead Letter Queue (SQS `gps-validate-dlq`)")
    dlq_ok = rob["dlq_visible"] == 0
    c1, c2 = st.columns(2)
    c1.metric(
        "Mensajes en DLQ",
        rob["dlq_visible"],
        help="Registros GPS que fallaron todos los reintentos. "
             "0 = pipeline saludable. >0 = revisar bronze_rejected/.",
        delta="✅ Pipeline saludable" if dlq_ok else f"⚠️ {rob['dlq_visible']} mensajes fallidos",
        delta_color="off" if dlq_ok else "inverse",
    )
    c2.metric(
        "Mensajes en procesamiento",
        rob["dlq_inflight"],
        help="Mensajes siendo procesados ahora mismo.",
    )
    if not dlq_ok:
        st.error(
            f"**{rob['dlq_visible']} registro(s) en DLQ** — fallaron todos los reintentos. "
            "Los archivos están en `s3://gps-bronze/bronze_rejected/` para auditoría."
        )

    # ── Registros rechazados ─────────────────────────────────────────────────
    st.markdown("#### 🗑️ Registros rechazados (bronze_rejected/)")
    c1, c2 = st.columns(2)
    c1.metric("Archivos GPS rechazados",          rob["gps_rejected"],
              help="Eventos GPS inválidos (coords fuera de Áncash, timestamp futuro, etc.)")
    c2.metric("Archivos mantenimiento rechazados", rob["mant_rejected"],
              help="Filas CSV con criticidad inválida, fecha malformada, etc.")

    if rob.get("rejected_samples"):
        with st.expander(f"Ver muestra de registros GPS rechazados ({len(rob['rejected_samples'])} más recientes)"):
            for r in rob["rejected_samples"]:
                reason = r.get("rejection_reason", "desconocido")
                eid    = r.get("equipo_id", r.get("_raw", "?"))[:20]
                st.code(
                    f"equipo_id: {eid}\n"
                    f"motivo:    {reason}\n"
                    f"lat/lon:   {r.get('latitude', r.get('latitud', '?'))} / "
                    f"{r.get('longitude', r.get('longitud', '?'))}",
                    language="yaml",
                )

    st.divider()

    # ── Estrategia de retry ─────────────────────────────────────────────────
    st.markdown("#### 🔁 Estrategia de retry (Kinesis → Lambda)")
    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("""
**Configuración en el Event Source Mapping:**

| Parámetro | Valor | Efecto |
|-----------|-------|--------|
| `maxRetryAttempts` | 2 | Reintenta hasta 2 veces antes de enviar a DLQ |
| `bisectBatchOnFunctionError` | `true` | Si falla un batch de 100, lo divide en dos y reintenta cada mitad → aísla el registro problemático sin bloquear los demás |
| `batchSize` | 100 | Eventos procesados por invocación Lambda |
| `maximumBatchingWindow` | 10 s | Agrupa eventos para reducir invocaciones |
| `destinationOnFailure` | SQS DLQ | Destino final de mensajes que agotaron reintentos |
        """)
    with col_r:
        st.markdown("""
**Flujo de un error:**

```
Kinesis → validate_gps Lambda
              ↓ falla
         Reintento 1 (batch completo)
              ↓ falla
         Reintento 2 (bisect: 50 + 50)
              ↓ falla
         Reintento 3 (bisect: 25 + 25...)
              ↓ agotado
         → SQS DLQ (gps-validate-dlq)
              ↓
         CloudWatch Alarm: DLQ depth > 0
```

**Idempotencia:** la deduplicación por
`equipo_id + timestamp` en DynamoDB
garantiza que un reintento no duplique
el mismo evento en silver/.
        """)

    st.divider()

    # ── Logging estructurado ─────────────────────────────────────────────────
    st.markdown("#### 📋 Logging estructurado (JSON → CloudWatch Logs)")
    st.markdown(
        "Todas las Lambdas usan `common/logger.py` que emite JSON estructurado. "
        "Ejemplo de log de un evento rechazado:"
    )
    st.code(json.dumps({
        "level":      "WARNING",
        "logger":     "validate_gps",
        "timestamp":  "2026-06-05T04:00:00",
        "message":    "Validation failed",
        "equipo_id":  "CAM_007",
        "reason":     "lat_out_of_bbox:72.29",
    }, indent=2), language="json")
    st.markdown(
        "Con JSON en CloudWatch, se pueden hacer queries como:\n\n"
        "```sql\n"
        "-- CloudWatch Logs Insights\n"
        "filter level = 'WARNING' and reason like 'lat_out_of_bbox'\n"
        "| stats count() by equipo_id\n"
        "```"
    )

    st.divider()

    # ── CloudWatch alarms ────────────────────────────────────────────────────
    st.markdown("#### 🔔 Alarmas CloudWatch configuradas")
    alarms = [
        {"Nombre":         "validate-gps-errors",
         "Métrica":        "Lambda Errors",
         "Umbral":         "> 5 en 1 min",
         "Acción":         "SNS gps-alertas",
         "Por qué":        "Errores de validación inesperados (bugs, timeouts)"},
        {"Nombre":         "detect-signal-loss-errors",
         "Métrica":        "Lambda Errors",
         "Umbral":         "> 1 en 5 min",
         "Acción":         "SNS gps-alertas",
         "Por qué":        "Si falla la detección de pérdida, nadie se entera de equipos caídos"},
        {"Nombre":         "kinesis-gps-iterator-age",
         "Métrica":        "IteratorAge P99",
         "Umbral":         "> 60,000 ms",
         "Acción":         "SNS gps-alertas",
         "Por qué":        "Indica que el consumer está atrasado — Lambda throttled o fallando"},
        {"Nombre":         "gps-validate-dlq-depth",
         "Métrica":        "SQS MessageCount",
         "Umbral":         "> 0",
         "Acción":         "SNS gps-alertas",
         "Por qué":        "Cualquier mensaje en DLQ significa pérdida de datos GPS"},
        {"Nombre":         "validate-gps-duration-p99",
         "Métrica":        "Lambda Duration P99",
         "Umbral":         "> 45,000 ms",
         "Acción":         "SNS gps-alertas",
         "Por qué":        "P99 > 45s de 60s timeout → Lambda está a punto de fallar por timeout"},
    ]
    st.dataframe(pd.DataFrame(alarms), use_container_width=True, hide_index=True)
    st.caption(
        "Definidas en `infra/terraform/monitoring.tf`. "
        "Todas apuntan a SNS `gps-alertas` que puede re-enviar a email, PagerDuty, Slack, etc."
    )
