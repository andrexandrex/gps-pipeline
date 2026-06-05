"""
GPS Pipeline — Streamlit Dashboard
Reads from LocalStack S3 + DynamoDB. No computation happens here — it only
visualises what run_pipeline.py or the Lambda functions already wrote.

How to start:
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

_ep = os.getenv("AWS_ENDPOINT_URL")
_kw = {"endpoint_url": _ep} if _ep else {}

SILVER        = os.getenv("SILVER_BUCKET",                 "gps-silver")
GOLD          = os.getenv("GOLD_BUCKET",                   "gps-gold")
DYNAMO        = os.getenv("DYNAMO_TABLE_NAME",             "gps-last-seen")
ALERT_MIN     = int(os.getenv("SIGNAL_LOSS_THRESHOLD_MINUTES",        "10"))
MAINT_MIN     = int(os.getenv("AUTO_MAINTENANCE_THRESHOLD_MINUTES",   "30"))
ENV_LABEL     = f"`{_ep}`" if _ep else "**AWS real**"


@st.cache_resource
def _s3():
    return boto3.client("s3", **_kw)

@st.cache_resource
def _dynamo_client():
    return boto3.client("dynamodb", **_kw)


# ── Data loaders (cached 30 s) ────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_quality_runs(dataset: str) -> pd.DataFrame:
    """
    Returns one row per quality run, sorted by timestamp.
    'total_rows' = how many rows were in silver when the check ran.
    This number grows over time as more data accumulates — that's normal.
    """
    prefix = f"quality_metrics/{dataset}/"
    try:
        resp = _s3().list_objects_v2(Bucket=GOLD, Prefix=prefix)
    except Exception:
        return pd.DataFrame()

    rows = []
    for obj in resp.get("Contents", []):
        if not obj["Key"].endswith(".json"):
            continue
        try:
            body = _s3().get_object(Bucket=GOLD, Key=obj["Key"])["Body"].read()
            m = json.loads(body)
            completeness = m.get("completeness_pct", {})
            rows.append({
                "Fecha/hora del check":      m.get("run_timestamp", "")[:19].replace("T", " "),
                "Registros en silver":       m.get("total_rows", 0),
                "% válidos":                 round(m.get("valid_pct", 0), 1),
                "% duplicados":              round(m.get("duplicates_pct", 0), 1),
                "Filas con error Pandera":   m.get("pandera_failures", 0),
                "Completitud mín (%)":       round(min(completeness.values(), default=100), 1),
                "_out_of_range":             m.get("out_of_range_pct", {}),
                "_completeness":             completeness,
            })
        except Exception:
            pass

    return pd.DataFrame(rows).sort_values("Fecha/hora del check") if rows else pd.DataFrame()


@st.cache_data(ttl=30)
def load_silver_parquet(prefix: str) -> pd.DataFrame:
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


@st.cache_data(ttl=15)
def load_equipment_status() -> pd.DataFrame:
    try:
        paginator = _dynamo_client().get_paginator("scan")
        rows = []
        for page in paginator.paginate(TableName=DYNAMO):
            for item in page.get("Items", []):
                eid  = item.get("equipo_id", {}).get("S", "")
                seen = item.get("last_seen",  {}).get("S", "")
                if seen:
                    try:
                        dt = datetime.fromisoformat(seen)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        mins = round((datetime.now(timezone.utc) - dt).total_seconds() / 60, 1)
                        if mins <= ALERT_MIN:
                            estado, icono = "OK", "🟢"
                        elif mins <= MAINT_MIN:
                            estado, icono = "ALERTA", "🟡"
                        else:
                            estado, icono = "SIN SEÑAL", "🔴"
                    except ValueError:
                        mins, estado, icono = None, "DATO INVÁLIDO", "⚠️"
                else:
                    mins, estado, icono = None, "SIN REGISTRO", "⚠️"
                rows.append({
                    "Equipo":              eid,
                    "Última señal GPS":    seen[:19].replace("T", " ") if seen else "—",
                    "Min sin señal":       mins,
                    "Estado":             f"{icono} {estado}",
                })
        df = pd.DataFrame(rows)
        return df.sort_values("Min sin señal", ascending=False, na_position="last") if not df.empty else df
    except Exception:
        return pd.DataFrame()


# ── Layout ────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="GPS Pipeline — Áncash, Perú", page_icon="🚛", layout="wide")

st.title("🚛 GPS Pipeline — Áncash, Perú")

connected = "🟢 LocalStack" if _ep else "🔴 AWS real (sin LocalStack)"
st.caption(
    f"Conexión: {connected} {ENV_LABEL}  |  "
    f"Alerta señal: **{ALERT_MIN} min**  |  Auto-mantenimiento: **{MAINT_MIN} min**  |  "
    f"Datos en S3 se actualizan cada 30 s — pulsa 🔄 para forzar"
)

if st.button("🔄 Refrescar datos"):
    st.cache_data.clear()
    st.rerun()

tab_status, tab_quality, tab_maintenance = st.tabs([
    "📡 Estado de Equipos",
    "📊 Calidad de Datos",
    "🔧 Mantenimientos",
])


# ═════════════════════════════════════════════════════
# TAB 1 — Estado de Equipos
# ═════════════════════════════════════════════════════
with tab_status:
    eq_df = load_equipment_status()

    if eq_df.empty:
        st.warning(
            "Sin datos. Corre el pipeline primero:\n\n"
            "```bash\n"
            "env $(cat .env | grep -v '^#' | xargs) \\\n"
            "PYTHONPATH=src:src/lambdas \\\n"
            "python3 scripts/run_pipeline.py --all\n"
            "```"
        )
    else:
        ok_df     = eq_df[eq_df["Estado"].str.contains("OK")]
        alert_df  = eq_df[eq_df["Estado"].str.contains("ALERTA")]
        lost_df   = eq_df[eq_df["Estado"].str.contains("SIN SEÑAL")]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total equipos rastreados", len(eq_df),
                  help="Dispositivos que enviaron al menos un evento GPS")
        c2.metric("🟢 Señal OK",  len(ok_df),
                  help=f"Señal recibida en los últimos {ALERT_MIN} min")
        c3.metric("🟡 En alerta", len(alert_df),
                  help=f"Sin señal entre {ALERT_MIN} y {MAINT_MIN} min → alerta SNS enviada")
        c4.metric("🔴 Sin señal", len(lost_df),
                  help=f"Sin señal >10 min → SNS + registro automático de mantenimiento creado",
                  delta=f"-{len(lost_df)}" if lost_df.empty else None,
                  delta_color="inverse")

        if not lost_df.empty:
            st.error(
                f"**{len(lost_df)} equipo(s) sin señal por más de {ALERT_MIN} min.**  "
                f"Se creó un registro de mantenimiento automático en silver/mantenimientos/ "
                f"para los que superan {MAINT_MIN} min:  \n"
                + "  ".join(f"`{e}`" for e in lost_df["Equipo"].tolist())
            )

        st.dataframe(
            eq_df,
            use_container_width=True,
            column_config={
                "Min sin señal": st.column_config.NumberColumn(
                    "Min sin señal", format="%.1f min"
                ),
            },
            hide_index=True,
        )

        # Bar chart: minutes without signal per device
        chart_df = eq_df.dropna(subset=["Min sin señal"]).set_index("Equipo")[["Min sin señal"]]
        if not chart_df.empty:
            st.subheader("Tiempo sin señal por equipo (minutos)")
            st.bar_chart(chart_df)
            st.caption(
                f"Línea de alerta SNS: {ALERT_MIN} min  |  "
                f"Línea de auto-mantenimiento: {MAINT_MIN} min"
            )


# ═════════════════════════════════════════════════════
# TAB 2 — Calidad de Datos
# ═════════════════════════════════════════════════════
with tab_quality:
    dataset_sel = st.radio(
        "Dataset a inspeccionar",
        ["gps_eventos", "mantenimientos"],
        horizontal=True,
        help="gps_eventos = flujo streaming | mantenimientos = CSV batch",
    )
    runs_df = load_quality_runs(dataset_sel)

    if runs_df.empty:
        st.info(
            "Sin métricas de calidad aún. Ejecuta:\n\n"
            "`python3 scripts/run_pipeline.py --quality`"
        )
    else:
        latest = runs_df.iloc[-1]

        # ── KPIs del último run ───────────────────────────────────────────────
        st.subheader("Último check de calidad")
        st.caption(f"Ejecutado: {latest['Fecha/hora del check']}")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(
            "Registros analizados",
            int(latest["Registros en silver"]),
            help="Cuántas filas había en silver/ cuando se ejecutó el check. "
                 "Este número crece con cada run del pipeline — es correcto.",
        )
        c2.metric(
            "% Registros válidos",
            f"{latest['% válidos']:.1f}%",
            help="Porcentaje de filas que pasaron todas las reglas Pandera. "
                 "< 95% indica un problema serio en la fuente de datos.",
            delta=f"{latest['% válidos'] - 100:.1f}%" if latest["% válidos"] < 100 else "✓ Sin errores",
            delta_color="inverse" if latest["% válidos"] < 100 else "off",
        )
        c3.metric(
            "% Duplicados detectados",
            f"{latest['% duplicados']:.1f}%",
            help="Porcentaje de filas con clave duplicada (equipo_id + timestamp). "
                 "Duplicados son normales si el pipeline se ejecutó varias veces con el mismo CSV.",
            delta=f"{latest['% duplicados']:.1f}% dup" if latest["% duplicados"] > 0 else "✓ Sin duplicados",
            delta_color="inverse" if latest["% duplicados"] > 5 else "off",
        )
        c4.metric(
            "Errores Pandera",
            int(latest["Filas con error Pandera"]),
            help="Número de celdas que violan una regla del schema "
                 "(bbox, rango de velocidad, fecha inválida, etc.).",
            delta="✓ Cero errores" if latest["Filas con error Pandera"] == 0 else None,
            delta_color="off",
        )

        # ── Tendencia histórica ───────────────────────────────────────────────
        if len(runs_df) > 1:
            st.subheader("Tendencia de calidad en el tiempo")
            trend_df = runs_df.set_index("Fecha/hora del check")[["% válidos", "% duplicados"]]
            st.line_chart(trend_df, color=["#2ecc71", "#e74c3c"])
            st.caption(
                "Verde = % válidos (meta: 100%)  |  "
                "Rojo = % duplicados (meta: 0%)  |  "
                "El eje X es el timestamp de cada check de calidad."
            )

        # ── Detalle por columna ───────────────────────────────────────────────
        completeness = latest.get("_completeness", {})
        out_of_range = latest.get("_out_of_range", {})

        if completeness:
            st.subheader("Completitud y rangos por columna")
            col_df = pd.DataFrame([
                {
                    "Columna":             col,
                    "% valores presentes": f"{pct:.1f}%",
                    "% fuera de rango":    f"{out_of_range.get(col, 0):.1f}%",
                    "Estado":              "✅ OK" if pct == 100 and out_of_range.get(col, 0) == 0
                                           else ("⚠️ revisar" if pct >= 95 else "🚨 crítico"),
                }
                for col, pct in completeness.items()
            ])
            st.dataframe(col_df, use_container_width=True, hide_index=True)
            st.caption(
                "Completitud = % de celdas con valor (no nulo). "
                "Fuera de rango = % de valores que violan los límites del schema "
                "(para GPS: bbox Áncash lat/lon, velocidad 0–200 km/h)."
            )

        # ── Historial de runs ─────────────────────────────────────────────────
        with st.expander("Ver todos los runs"):
            display_df = runs_df[[
                "Fecha/hora del check", "Registros en silver",
                "% válidos", "% duplicados", "Filas con error Pandera",
            ]].copy()
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            st.caption(
                "**Por qué 'Registros en silver' crece:** el quality checker escanea TODOS "
                "los Parquet en silver/ cada vez que corre. Si corres el pipeline 5 veces, "
                "acumulas 5 × 50 = 250 filas. Esto es intencional — mide la salud de TODA "
                "la data histórica, no solo la del último lote."
            )


# ═════════════════════════════════════════════════════
# TAB 3 — Mantenimientos
# ═════════════════════════════════════════════════════
with tab_maintenance:
    mant_df = load_silver_parquet("mantenimientos/")

    if mant_df.empty:
        st.info(
            "Sin datos de mantenimiento. Ejecuta:\n\n"
            "`python3 scripts/run_pipeline.py --batch`"
        )
    else:
        # Separate auto-generated GPS loss records from manual maintenance
        is_auto = mant_df["tipo_falla"].str.contains("GPS DESCONECTADO", na=False) \
                  if "tipo_falla" in mant_df.columns else pd.Series(False, index=mant_df.index)
        manual_df = mant_df[~is_auto]
        auto_df   = mant_df[is_auto]

        alta_df = mant_df[mant_df.get("criticidad", pd.Series()) == "ALTA"] \
                  if "criticidad" in mant_df.columns else pd.DataFrame()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total registros",    len(mant_df),
                  help="Manual + auto-generados por pérdida de GPS")
        c2.metric("🔴 Criticidad ALTA", len(alta_df),
                  help="Fallas con criticidad=ALTA, incluye GPS desconectado")
        c3.metric("⚙️ Manuales (CSV)",   len(manual_df),
                  help="Registros ingresados por los técnicos vía CSV")
        c4.metric("🤖 Auto GPS-loss",    len(auto_df),
                  help=f"Creados automáticamente cuando el equipo supera {MAINT_MIN} min sin señal")

        # ── Fallas críticas por equipo (replica query Athena) ────────────────
        if "criticidad" in mant_df.columns and "equipo_id" in mant_df.columns:
            fallas_df = (
                mant_df[mant_df["criticidad"] == "ALTA"]
                .groupby("equipo_id")
                .size()
                .reset_index(name="Fallas ALTA")
                .sort_values("Fallas ALTA", ascending=False)
            )
            if not fallas_df.empty:
                st.subheader("Fallas de criticidad ALTA por equipo")
                st.bar_chart(fallas_df.set_index("equipo_id"))
                at_risk = fallas_df[fallas_df["Fallas ALTA"] > 3]
                if not at_risk.empty:
                    st.error(
                        f"🚨 **{len(at_risk)} equipo(s) con >3 fallas ALTA** "
                        f"— coinciden con el filtro del query Athena:  \n"
                        + "  ".join(f"`{e}` ({n})" for e, n in zip(at_risk["equipo_id"], at_risk["Fallas ALTA"]))
                    )

        # ── Auto-generated GPS loss records ───────────────────────────────────
        if not auto_df.empty:
            with st.expander(f"Ver {len(auto_df)} registros auto-generados por pérdida de señal"):
                st.dataframe(
                    auto_df[["equipo_id", "fecha_mantenimiento", "tipo_falla", "criticidad"]],
                    use_container_width=True, hide_index=True,
                )
                st.caption(
                    "Estos registros fueron creados automáticamente por `detect_signal_loss` "
                    f"cuando un equipo superó {MAINT_MIN} min sin enviar señal GPS. "
                    "Se tratan igual que los manuales en el query Athena."
                )

        # ── Manual records ─────────────────────────────────────────────────────
        if not manual_df.empty:
            with st.expander(f"Ver {len(manual_df)} registros manuales (CSV)", expanded=True):
                cols = [c for c in ["equipo_id", "fecha_mantenimiento", "tipo_falla",
                                    "criticidad"] if c in manual_df.columns]
                st.dataframe(manual_df[cols], use_container_width=True, hide_index=True)
