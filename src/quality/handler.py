"""
Lambda: quality_checker
Trigger: EventBridge Scheduler daily, or manual invocation.

Reads silver/ datasets, runs Pandera validation, writes quality metrics to gold/.
Can also be called inline from other Lambdas by passing a DataFrame directly.
"""

import os

from common.logger import get_logger
from quality.checker import read_silver, validate, write_metrics

logger = get_logger("quality.handler")


def handler(event: dict, context) -> dict:
    silver_bucket = os.getenv("SILVER_BUCKET", "gps-silver")
    gold_bucket   = os.getenv("GOLD_BUCKET",   "gps-gold")

    # event can specify which datasets to check; default = all
    datasets = event.get("datasets", ["gps_eventos", "mantenimientos"])

    results = []
    for dataset in datasets:
        prefix_map = {
            "gps_eventos":    "gps_eventos/",
            "mantenimientos": "mantenimientos/",
        }
        prefix = prefix_map.get(dataset)
        if not prefix:
            logger.warning("Unknown dataset, skipping", extra={"dataset": dataset})
            continue

        logger.info("Reading silver", extra={"dataset": dataset, "prefix": prefix})
        df = read_silver(silver_bucket, prefix)

        if df.empty:
            logger.warning("No data found in silver", extra={"dataset": dataset})
            results.append({"dataset": dataset, "status": "no_data"})
            continue

        _, _, metrics = validate(df, dataset)
        json_key = write_metrics(metrics, gold_bucket)

        results.append({
            "dataset":       dataset,
            "status":        "ok",
            "total_rows":    metrics["total_rows"],
            "valid_pct":     metrics["valid_pct"],
            "duplicates_pct": metrics["duplicates_pct"],
            "gold_key":      json_key,
        })

    logger.info("Quality run complete", extra={"datasets_checked": len(results)})
    return {"checked": len(results), "results": results}
