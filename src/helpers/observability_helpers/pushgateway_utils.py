# pushgateway_utils.py

from decouple import config
from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram, push_to_gateway

from src.helpers.logging_helpers.combine_loggers_helper import get_logger
# ВНИМАНИЕ: коригирай пътя според това къде реално си сложил/а prometheus_remote_write.py
from src.helpers.observability_helpers.prometheus_remote_write import push_metrics, build_timeseries

# Optional: configure Pushgateway URL via environment variable
PUSHGATEWAY_URL = config("PUSHGATEWAY_URL", default="localhost:9091")

# Локално (docker-compose / .env) -> True -> ползва Pushgateway (непроменено поведение).
# В Azure променливата липсва -> default False -> праща директно remote write към Mimir.
PUSHGATEWAY_ENABLED = config("PUSHGATEWAY_ENABLED", default=False, cast=bool)


def push_metrics_to_gateway(flow_name: str, status: str, duration: float, pushgateway_url: str = PUSHGATEWAY_URL):
    """
    Push flow-level metrics to Prometheus Pushgateway (локално) или
    директно remote write към Grafana Cloud Mimir (Azure).

    Metrics pushed:
      - Flow runs total (success/failed)
      - Flow execution duration
      - Pipeline running status
    """
    logger = get_logger()

    if PUSHGATEWAY_ENABLED:
        registry = CollectorRegistry()

        flow_runs = Counter(
            "etl_flow_runs_total",
            "Total ETL flow runs",
            ["flow_name", "status"],
            registry=registry
        )
        flow_runs.labels(flow_name=flow_name, status=status).inc()

        flow_duration = Histogram(
            "etl_flow_duration_seconds",
            "ETL flow execution duration in seconds",
            ["flow_name"],
            registry=registry
        )
        flow_duration.labels(flow_name=flow_name).observe(duration)

        pipeline_running = Gauge(
            "etl_pipeline_running",
            "ETL pipeline running status (0=finished, 1=running)",
            ["flow_name"],
            registry=registry
        )
        pipeline_running.labels(flow_name=flow_name).set(0)

        try:
            push_to_gateway(pushgateway_url, job=flow_name, registry=registry)
        except Exception as e:
            logger.warning(f"Failed to push metrics to pushgateway: {e}")
    else:
        timeseries = [
            build_timeseries("etl_flow_runs_total", 1, {"flow_name": flow_name, "status": status}),
            build_timeseries("etl_flow_duration_seconds", duration, {"flow_name": flow_name}),
            build_timeseries("etl_pipeline_running", 0, {"flow_name": flow_name}),
        ]
        if not push_metrics(timeseries):
            logger.warning("Failed to push metrics via remote write.")


def push_task_metrics(
        flow_name: str,
        task_name: str,
        duration: float,
        pushgateway_url: str = PUSHGATEWAY_URL
):
    """
    Push task-level metrics to Prometheus Pushgateway / remote write.

    Metrics pushed:
      - Task execution duration
    """
    logger = get_logger()

    if PUSHGATEWAY_ENABLED:
        registry = CollectorRegistry()

        task_duration = Histogram(
            "etl_task_duration_seconds",
            "ETL task execution duration in seconds",
            ["flow_name", "task_name"],
            registry=registry
        )
        task_duration.labels(flow_name, task_name).observe(duration)

        try:
            push_to_gateway(pushgateway_url, job=f"{flow_name}_{task_name}", registry=registry)
        except Exception as e:
            logger.warning(f"Failed to push metrics to pushgateway: {e}")
    else:
        ts = build_timeseries(
            "etl_task_duration_seconds",
            duration,
            {"flow_name": flow_name, "task_name": task_name}
        )
        if not push_metrics([ts]):
            logger.warning("Failed to push metrics via remote write.")


def push_api_metrics(
        api_name: str,
        location: str,
        duration: float,
        pushgateway_url: str = PUSHGATEWAY_URL
):
    """
    Push call-level metrics to Prometheus Pushgateway / remote write.

    Metrics pushed:
      - API call execution duration
    """
    logger = get_logger()

    if PUSHGATEWAY_ENABLED:
        registry = CollectorRegistry()

        call_duration = Histogram(
            "api_call_duration_seconds",
            "API call execution duration in seconds",
            ["api_name", "location"],
            registry=registry
        )
        call_duration.labels(api_name, location).observe(duration)

        try:
            push_to_gateway(pushgateway_url, job="weather_pipeline", registry=registry)
        except Exception as e:
            logger.warning(f"Failed to push metrics to pushgateway: {e}")
    else:
        ts = build_timeseries(
            "api_call_duration_seconds",
            duration,
            {"api_name": api_name, "location": location}
        )
        if not push_metrics([ts]):
            logger.warning("Failed to push metrics via remote write.")


def push_processing_state_metrics(
        flow_name: str,
        rows: list[dict],
        pushgateway_url: str = PUSHGATEWAY_URL
):
    """
    Push processing-state metrics (completeness, retries, acceptability, errors)
    to Prometheus Pushgateway / remote write.
    """
    logger = get_logger()

    if PUSHGATEWAY_ENABLED:
        registry = CollectorRegistry()

        completeness = Gauge(
            "etl_completeness_ratio",
            "Completeness ratio по processing_level",
            ["flow_name", "processing_level", "status"],
            registry=registry
        )
        retries = Gauge(
            "etl_retry_count",
            "Retry count по processing_level",
            ["flow_name", "processing_level", "status"],
            registry=registry
        )
        acceptable = Gauge(
            "etl_is_acceptable",
            "Дали последният partition е приемлив (1=да, 0=не)",
            ["flow_name", "processing_level"],
            registry=registry
        )
        errors = Counter(
            "etl_processing_errors_total",
            "Брой грешки по тип",
            ["flow_name", "processing_level", "error_type"],
            registry=registry
        )

        for row in rows:
            level = row["processing_level"]
            status = row["status"]

            if row["completeness_ratio"] is not None:
                completeness.labels(flow_name, level, status).set(row["completeness_ratio"])

            retries.labels(flow_name, level, status).set(row["retry_count"])
            acceptable.labels(flow_name, level).set(1 if row["is_acceptable"] else 0)

            if row["error_type"]:
                errors.labels(flow_name, level, row["error_type"]).inc()

        try:
            push_to_gateway(pushgateway_url, job=f"{flow_name}_processing_state", registry=registry)
        except Exception as e:
            logger.warning(f"Failed to push metrics to pushgateway: {e}")
    else:
        timeseries = []
        for row in rows:
            level = row["processing_level"]
            status = row["status"]

            if row["completeness_ratio"] is not None:
                timeseries.append(build_timeseries(
                    "etl_completeness_ratio",
                    row["completeness_ratio"],
                    {"flow_name": flow_name, "processing_level": level, "status": status}
                ))

            timeseries.append(build_timeseries(
                "etl_retry_count",
                row["retry_count"],
                {"flow_name": flow_name, "processing_level": level, "status": status}
            ))

            timeseries.append(build_timeseries(
                "etl_is_acceptable",
                1 if row["is_acceptable"] else 0,
                {"flow_name": flow_name, "processing_level": level}
            ))

            if row["error_type"]:
                timeseries.append(build_timeseries(
                    "etl_processing_errors_total",
                    1,
                    {"flow_name": flow_name, "processing_level": level, "error_type": row["error_type"]}
                ))

        if timeseries and not push_metrics(timeseries):
            logger.warning("Failed to push metrics via remote write.")


def push_api_error_metrics(
        api_name: str,
        location: str,
        error_type: str,
        pushgateway_url: str = PUSHGATEWAY_URL
):
    """
    Push API error event to Prometheus Pushgateway / remote write.
    """
    logger = get_logger()

    if PUSHGATEWAY_ENABLED:
        registry = CollectorRegistry()

        api_errors = Counter(
            "api_errors_total",
            "Total API call errors",
            ["api_name", "location", "error_type"],
            registry=registry
        )
        api_errors.labels(api_name, location, error_type).inc()

        try:
            push_to_gateway(pushgateway_url, job="weather_pipeline", registry=registry)
        except Exception as e:
            logger.warning(f"Failed to push metrics to pushgateway: {e}")
    else:
        ts = build_timeseries(
            "api_errors_total",
            1,
            {"api_name": api_name, "location": location, "error_type": error_type}
        )
        if not push_metrics([ts]):
            logger.warning("Failed to push metrics via remote write.")




























# # pushgateway_utils.py
#
# from decouple import config
# from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram, push_to_gateway
#
# from src.helpers.logging_helpers.combine_loggers_helper import get_logger
#
# # Optional: configure Pushgateway URL via environment variable
# PUSHGATEWAY_URL = config("PUSHGATEWAY_URL", default="localhost:9091")
#
#
# def push_metrics_to_gateway(flow_name: str, status: str, duration: float, pushgateway_url: str = PUSHGATEWAY_URL):
#     """
#     Push flow-level metrics to Prometheus Pushgateway.
#
#     Metrics pushed:
#       - Flow runs total (success/failed)
#       - Flow execution duration
#       - Pipeline running status
#     """
#     registry = CollectorRegistry()
#     logger = get_logger()
#
#     # Flow run counter (success / failed)
#     flow_runs = Counter(
#         "etl_flow_runs_total",
#         "Total ETL flow runs",
#         ["flow_name", "status"],
#         registry=registry
#     )
#     flow_runs.labels(flow_name=flow_name, status=status).inc()
#
#     # Flow execution duration
#     flow_duration = Histogram(
#         "etl_flow_duration_seconds",
#         "ETL flow execution duration in seconds",
#         ["flow_name"],
#         registry=registry
#     )
#     flow_duration.labels(flow_name=flow_name).observe(duration)
#
#     # Pipeline running status (0 = finished)
#     pipeline_running = Gauge(
#         "etl_pipeline_running",
#         "ETL pipeline running status (0=finished, 1=running)",
#         ["flow_name"],
#         registry=registry
#     )
#     pipeline_running.labels(flow_name=flow_name).set(0)
#
#     # Push metrics to Pushgateway
#     try:
#         push_to_gateway(pushgateway_url, job=flow_name, registry=registry)
#     except Exception as e:
#         logger.warning(f"Failed to push metrics to pushgateway: {e}")
#
# def push_task_metrics(
#         flow_name: str,
#         task_name: str,
#         duration: float,
#         pushgateway_url: str = PUSHGATEWAY_URL
# ):
#     """
#     Push task-level metrics to Prometheus Pushgateway.
#
#     Metrics pushed:
#       - Task execution duration
#       - Rows processed
#     """
#     registry = CollectorRegistry()
#     logger = get_logger()
#
#     # Task duration
#     task_duration = Histogram(
#         "etl_task_duration_seconds",
#         "ETL task execution duration in seconds",
#         ["flow_name", "task_name"],
#         registry=registry
#     )
#     task_duration.labels(flow_name, task_name).observe(duration)
#
#     # Rows processed
#     # rows_processed = Counter(
#     #     "etl_rows_processed_total",
#     #     "Number of rows processed per task",
#     #     ["flow_name", "task_name"],
#     #     registry=registry
#     # )
#     # rows_processed.labels(flow_name, task_name).inc(rows)
#
#     # Push metrics to Pushgateway
#     try:
#         push_to_gateway(pushgateway_url, job=f"{flow_name}_{task_name}", registry=registry)
#     except Exception as e:
#         logger.warning(f"Failed to push metrics to pushgateway: {e}")
#
#
# def push_api_metrics(
#         api_name: str,
#         location: str,
#         duration: float,
#         pushgateway_url: str = PUSHGATEWAY_URL
# ):
#     """
#     Push call-level metrics to Prometheus Pushgateway.
#
#     Metrics pushed:
#       - API name
#       - Searched location
#       - Call execution duration
#
#     """
#     registry = CollectorRegistry()
#     logger = get_logger()
#
#     # Task duration
#     call_duration = Histogram(
#         "api_call_duration_seconds",
#         "API call execution duration in seconds",
#         ["api_name", "location"],
#         registry=registry
#     )
#     call_duration.labels(api_name, location).observe(duration)
#
#     # Push metrics to Pushgateway
#     try:
#         push_to_gateway(pushgateway_url, job="weather_pipeline", registry=registry)
#     except Exception as e:
#         logger.warning(f"Failed to push metrics to pushgateway: {e}")
#
#
# def push_processing_state_metrics(
#         flow_name: str,
#         rows: list[dict],
#         pushgateway_url: str = PUSHGATEWAY_URL
# ):
#     registry = CollectorRegistry()
#     logger = get_logger()
#
#     completeness = Gauge(
#         "etl_completeness_ratio",
#         "Completeness ratio по processing_level",
#         ["flow_name", "processing_level", "status"],
#         registry=registry
#     )
#     retries = Gauge(
#         "etl_retry_count",
#         "Retry count по processing_level",
#         ["flow_name", "processing_level", "status"],
#         registry=registry
#     )
#     acceptable = Gauge(
#         "etl_is_acceptable",
#         "Дали последният partition е приемлив (1=да, 0=не)",
#         ["flow_name", "processing_level"],
#         registry=registry
#     )
#     errors = Counter(
#         "etl_processing_errors_total",
#         "Брой грешки по тип",
#         ["flow_name", "processing_level", "error_type"],
#         registry=registry
#     )
#
#     for row in rows:
#         level = row["processing_level"]
#         status = row["status"]
#
#         if row["completeness_ratio"] is not None:
#             completeness.labels(flow_name, level, status).set(row["completeness_ratio"])
#
#         retries.labels(flow_name, level, status).set(row["retry_count"])
#
#         acceptable.labels(flow_name, level).set(1 if row["is_acceptable"] else 0)
#
#         if row["error_type"]:
#             errors.labels(flow_name, level, row["error_type"]).inc()
#
#     try:
#         push_to_gateway(pushgateway_url, job=f"{flow_name}_processing_state", registry=registry)
#     except Exception as e:
#         logger.warning(f"Failed to push metrics to pushgateway: {e}")
#
#
#
# def push_api_error_metrics(
#         api_name: str,
#         location: str,
#         error_type: str,
#         pushgateway_url: str = PUSHGATEWAY_URL
#     ):
#     registry = CollectorRegistry()
#     logger = get_logger()
#
#     api_errors = Counter(
#         "api_errors_total",
#         "Total API call errors",
#         ["api_name", "location", "error_type"],  # error_type: "retryable" / "non_retryable" / "empty_response"
#         registry=registry
#     )
#
#     api_errors.labels(api_name, location, error_type).inc()
#
#     # Push metrics to Pushgateway
#     try:
#         push_to_gateway(pushgateway_url, job="weather_pipeline", registry=registry)
#     except Exception as e:
#         logger.warning(f"Failed to push metrics to pushgateway: {e}")
