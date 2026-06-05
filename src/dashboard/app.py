"""
GPS Pipeline — Streamlit Dashboard
Local alternative to QuickSight; reads directly from LocalStack S3 + DynamoDB.

Run:
    streamlit run src/dashboard/app.py

Env vars (same as the rest of the pipeline):
    AWS_ENDPOINT_URL=http://localhost:4566
    SILVER_BUCKET=gps-silver
    GOLD_BUCKET=gps-gold
    DYNAMO_TABLE_NAME=gps-last-seen
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

# ── AWS clients ───────────────────────────────────────────────────────────────
_ep = os.getenv("AWS_ENDPOINT_URL")
_kw = {"endpoint_url": _ep} if _ep else {}

@st.cache_resource
def _s3():
    return boto3.client("s3", **_kw)

@st.cache_resource
def _dynamo():
    return boto3.client("dynamodb", **_kw)

SILVER = os.getenv("SILVER_BUCKET", "gps-silver")
GOLD   = os.getenv("GOLD_BUCKET",   "gps-gold")
DYNAMO = os.getenv("DYNAMO_TABLE_NAME", "gps-last-seen")
THRESHOLD_MIN = int(os.getenv("SIGNAL_LOSS_THRESHOLD_MINUTES", "10"))

# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_quality_metrics(dataset: str) -> list[dict]:
    """Read all JSON quality metric files for a dataset from gold/."""
    prefix = f"quality_metrics/{dataset}/"
    try:
        resp = _s3().list_objects_v2(Bucket=GOLD, Prefix=prefix)
    except Exception:
        return []
    items = []
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".json"):
            try:
                body = _s3().get_object(Bucket=GOLD, Key=obj["Key"])["Body"].read()
                items.append(json.loads(body))
            except Exception:
                pass
    return sorted(items, key=lambda x: x.get("run_timestamp", ""))


@st.cache_data(ttl=30)
def load_silver_parquet(prefix: str) -> pd.DataFrame:
    """Read all Parquet files from silver/ under a given prefix."""
    try:
        paginator = _s3().get_paginator("list_objects_v2")
        frames = []
        for page in paginator.paginate(Bucket=SILVER, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    body = _s3().get_object(Bucket=SILVER, Key=obj["Key"])["Body"].read()
                    frames.append(pd.read_parquet(io.BytesIO(body)))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_equipment_status() -> pd.DataFrame:
    """Scan gps-last-seen DynamoDB and compute signal status per device."""
    try:
        paginator = _dynamo().get_paginator("scan")
        rows = []
        for page in paginator.paginate(TableName=DYNAMO):
            for item in page.get("Items", []):
                equipo_id   = item.get("equipo_id", {}).get("S", "")
                last_seen_s = item.get("last_seen",  {}).get("S", "")
                if last_seen_s:
                    try:
                        last_dt = datetime.fromisoformat(last_seen_s)
                        if last_dt.tzinfo is None:
                            last_dt = last_dt.replace(tzinfo=timezone.utc)
                        mins = round((datetime.now(timezone.utc) - last_dt).total_seconds() / 60, 1)
                        status = "🟢 OK" if mins <= THRESHOLD_MIN else "🔴 SIN SEÑAL"
                    except ValueError:
                        mins, status = None, "⚠️ DATO INVÁLIDO"
                else:
                    mins, status = None, "⚠️ SIN REGISTRO"
                rows.append({"equipo_id": equipo_id, "last_seen": last_seen_s,
                             "minutos_sin_señal": mins, "estado": status})
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


# ── Page layout ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="GPS Pipeline — Dashboard",
    page_icon="🚛",
    layout="wide",
)

st.title("🚛 GPS Pipeline — Áncash, Perú")
st.caption(f"LocalStack endpoint: `{_ep or 'AWS real'}`  |  Threshold señal: {THRESHOLD_MIN} min")

tab_quality, tab_status, tab_maintenance = st.tabs([
    "📊 Calidad de Datos",
    "📡 Estado de Equipos",
    "🔧 Mantenimientos",
])

# ── TAB 1: Quality metrics ────────────────────────────────────────────────────
with tab_quality:
    st.subheader("Métricas de calidad — último run")
    dataset_sel = st.radio("Dataset", ["gps_eventos", "mantenimientos"], horizontal=True)
    metrics_list = load_quality_metrics(dataset_sel)

    if not metrics_list:
        st.info("No hay métricas en gold/ todavía. Corre el quality_checker Lambda o el script de calidad.")
    else:
        latest = metrics_list[-1]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total filas",    latest.get("total_rows", 0))
        col2.metric("Válidas %",      f"{latest.get('valid_pct', 0):.1f}%")
        col3.metric("Duplicados %",   f"{latest.get('duplicates_pct', 0):.1f}%")
        col4.metric("Timestamp",      latest.get("run_timestamp", "")[:19])

        # Completeness bar chart
        completeness = latest.get("completeness_pct", {})
        if completeness:
            st.subheader("Completitud por columna (%)")
            st.bar_chart(
                pd.DataFrame.from_dict(completeness, orient="index", columns=["completitud_%"])
            )

        # Out-of-range
        oor = latest.get("out_of_range_pct", {})
        if oor:
            st.subheader("Fuera de rango por columna (%)")
            st.bar_chart(
                pd.DataFrame.from_dict(oor, orient="index", columns=["fuera_de_rango_%"])
            )

        # History table
        if len(metrics_list) > 1:
            st.subheader("Histórico de runs")
            hist_df = pd.DataFrame([{
                "timestamp":    m.get("run_timestamp", "")[:19],
                "total":        m.get("total_rows", 0),
                "válidas_%":    m.get("valid_pct", 0),
                "duplicados_%": m.get("duplicates_pct", 0),
                "failures":     m.get("pandera_failures", 0),
            } for m in metrics_list])
            st.dataframe(hist_df, use_container_width=True)

# ── TAB 2: Equipment status ───────────────────────────────────────────────────
with tab_status:
    st.subheader("Estado de señal GPS por equipo")

    if st.button("🔄 Actualizar"):
        st.cache_data.clear()

    eq_df = load_equipment_status()
    if eq_df.empty:
        st.info("No hay datos en DynamoDB. Corre el producer y espera unos segundos.")
    else:
        sin_senal = eq_df[eq_df["estado"].str.contains("SIN SEÑAL", na=False)]
        ok        = eq_df[eq_df["estado"].str.contains("OK", na=False)]

        c1, c2, c3 = st.columns(3)
        c1.metric("Total equipos",  len(eq_df))
        c2.metric("🟢 Con señal",   len(ok))
        c3.metric("🔴 Sin señal",   len(sin_senal), delta=f"-{len(sin_senal)}", delta_color="inverse")

        st.dataframe(
            eq_df.sort_values("minutos_sin_señal", ascending=False, na_position="last"),
            use_container_width=True,
        )

        if not sin_senal.empty:
            st.error(f"⚠️ **{len(sin_senal)} equipo(s) sin señal por más de {THRESHOLD_MIN} minutos:**  "
                     + ", ".join(sin_senal["equipo_id"].tolist()))

# ── TAB 3: Maintenance summary ────────────────────────────────────────────────
with tab_maintenance:
    st.subheader("Resumen de mantenimientos (silver/)")
    mant_df = load_silver_parquet("mantenimientos/")

    if mant_df.empty:
        st.info("No hay datos de mantenimiento en silver/. Sube un CSV a bronze/mantenimientos/.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total registros", len(mant_df))
        criticas = mant_df[mant_df["tipo_falla"] == "CRITICA"] if "tipo_falla" in mant_df.columns else pd.DataFrame()
        c2.metric("Fallas críticas", len(criticas))
        c3.metric("Equipos únicos",
                  mant_df["equipo_id"].nunique() if "equipo_id" in mant_df.columns else 0)

        # Equipos con >3 fallas críticas (replicates Athena query locally)
        if not criticas.empty and "equipo_id" in criticas.columns:
            top_fallas = (
                criticas.groupby("equipo_id")
                .size()
                .reset_index(name="fallas_criticas")
                .query("fallas_criticas > 3")
                .sort_values("fallas_criticas", ascending=False)
            )
            if not top_fallas.empty:
                st.subheader("🚨 Equipos con >3 fallas críticas")
                st.dataframe(top_fallas, use_container_width=True)

        st.subheader("Detalle")
        st.dataframe(mant_df, use_container_width=True)
