"""
Smoke test за Prometheus remote write връзката към Grafana Cloud Mimir.

Не пуска ETL flow - само праща една тестова метрика директно, за да
провериш, че credentials-ите и encoding-ът работят, преди да го
вържеш в реалния pipeline.

Употреба:
    export PROMETHEUS_REMOTE_WRITE_URL="https://prometheus-prod-58-prod-eu-central-0.grafana.net/api/prom/push"
    export PROMETHEUS_USERNAME="<от Key Vault: prometheus-username>"
    export PROMETHEUS_PASSWORD="<от Key Vault: prometheus-password>"
    python test_remote_write.py

(или ги сложи в локален .env и го зареди преди да пуснеш скрипта)
"""

import logging
import sys
import time

# ВНИМАНИЕ: коригирай импорта според реалния път до файла в твоя repo
from src.helpers.observability_helpers.prometheus_remote_write import push_metrics, build_timeseries

logging.basicConfig(level=logging.DEBUG)


def main():
    metric_name = "smoke_test_metric"
    test_value = 42
    labels = {"source": "smoke_test", "environment": "local"}

    print(f"Пращам тестова метрика: {metric_name}={test_value} labels={labels}")

    ts = build_timeseries(metric_name, test_value, labels)
    success = push_metrics([ts])

    if success:
        print("\n✅ Пратено успешно (HTTP 2xx от Mimir).")
        print("Провери в https://grafana.net -> Explore -> Metrics browser")
        print(f"   Търси метрика с име: {metric_name}")
        print("   (изчакай ~30-60 сек преди да се появи, заради ingestion delay)")
    else:
        print("\n❌ Push-ът неуспя. Виж DEBUG/WARNING логовете по-горе за причината.")
        print("   Най-чести причини:")
        print("   - PROMETHEUS_REMOTE_WRITE_URL / USERNAME / PASSWORD не са зададени")
        print("   - грешен username/password (401/403)")
        print("   - липсва python-snappy инсталация (import грешка по-горе)")
        sys.exit(1)


if __name__ == "__main__":
    main()