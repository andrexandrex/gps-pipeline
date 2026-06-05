# ── QuickSight (AWS real only — LocalStack no soporta QuickSight) ─────────────
# Requisito previo: habilitar QuickSight en la cuenta AWS desde la consola.
# Costo: ~$18/mes por autor, gratis el primer mes.
# Alternativa local: Streamlit (src/dashboard/app.py) — cero costo, cero licencias.

locals {
  qs_enabled = !var.use_localstack
}

# ── Data source: Athena ───────────────────────────────────────────────────────
resource "aws_quicksight_data_source" "athena" {
  count          = local.qs_enabled ? 1 : 0
  aws_account_id = data.aws_caller_identity.current.account_id
  data_source_id = "gps-pipeline-athena"
  name           = "GPS Pipeline — Athena"
  type           = "ATHENA"

  parameters {
    athena {
      work_group = "primary"
    }
  }

  ssl_properties {
    disable_ssl = false
  }

  permission {
    actions   = ["quicksight:DescribeDataSource", "quicksight:DescribeDataSourcePermissions",
                 "quicksight:PassDataSource", "quicksight:UpdateDataSource"]
    principal = "arn:aws:quicksight:${var.aws_region}:${data.aws_caller_identity.current.account_id}:user/default/Admin"
  }
}

# ── Dataset: quality metrics ──────────────────────────────────────────────────
resource "aws_quicksight_dataset" "quality_metrics" {
  count          = local.qs_enabled ? 1 : 0
  aws_account_id = data.aws_caller_identity.current.account_id
  data_set_id    = "gps-quality-metrics"
  name           = "GPS Quality Metrics"
  import_mode    = "SPICE"   # caches data in QuickSight — faster, fixed cost

  physical_table_map {
    physical_table_map_id = "quality_metrics_table"
    relational_table {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      catalog         = "AwsDataCatalog"
      schema          = "gps_pipeline"
      name            = "quality_metrics"
      input_columns {
        name = "dataset"       ; type = "STRING"
      }
      input_columns {
        name = "run_timestamp" ; type = "STRING"
      }
      input_columns {
        name = "total_rows"    ; type = "INTEGER"
      }
      input_columns {
        name = "valid_pct"     ; type = "DECIMAL"
      }
      input_columns {
        name = "duplicates_pct"; type = "DECIMAL"
      }
      input_columns {
        name = "pandera_failures"; type = "INTEGER"
      }
    }
  }
}

# ── Dataset: equipos con fallas críticas (query Athena) ─────────────────────
resource "aws_quicksight_dataset" "equipos_fallas" {
  count          = local.qs_enabled ? 1 : 0
  aws_account_id = data.aws_caller_identity.current.account_id
  data_set_id    = "gps-equipos-fallas"
  name           = "GPS Equipos — Fallas Críticas"
  import_mode    = "SPICE"

  physical_table_map {
    physical_table_map_id = "equipos_fallas_table"
    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      name            = "equipos_fallas_query"
      sql_query       = file("${path.module}/../../sql/equipos_fallas_criticas.sql")
      columns {
        name = "equipo_id"            ; type = "STRING"
      }
      columns {
        name = "total_fallas_criticas"; type = "INTEGER"
      }
      columns {
        name = "fallas_pendientes"    ; type = "INTEGER"
      }
      columns {
        name = "ultimo_mantenimiento" ; type = "STRING"
      }
      columns {
        name = "ultima_fecha_gps"     ; type = "DATETIME"
      }
      columns {
        name = "estado_gps"           ; type = "STRING"
      }
      columns {
        name = "minutos_sin_senal"    ; type = "INTEGER"
      }
    }
  }
}

# ── Referencia a la cuenta actual (para ARNs) ─────────────────────────────────
data "aws_caller_identity" "current" {}

# ── Nota de despliegue ────────────────────────────────────────────────────────
# Para activar QuickSight en AWS real:
# 1. Ir a https://quicksight.aws.amazon.com y subscribirse (Enterprise edition)
# 2. Crear usuario Admin en QuickSight → actualizar el principal ARN arriba
# 3. terraform apply -var="use_localstack=false"
# 4. En QuickSight console: crear análisis desde los datasets creados aquí
# 5. Publicar como Dashboard y compartir con el equipo
