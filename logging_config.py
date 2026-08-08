import logging
import logging.config
import os
from pathlib import Path

LOG_DIR = Path("src/logs")
LOG_DIR.mkdir(exist_ok=True)

LOKI_URL = os.getenv("LOKI_URL")            # напр. http://localhost:3100 или https://logs-prod-XXX.grafana.net
LOKI_USERNAME = os.getenv("LOKI_USERNAME")  # само за Grafana Cloud
LOKI_PASSWORD = os.getenv("LOKI_PASSWORD")  # само за Grafana Cloud
JOB_NAME = os.getenv("JOB_NAME", "unknown-job")
ENVIRONMENT = os.getenv("ENVIRONMENT", "unknown")

LOKI_ENABLED = bool(LOKI_URL)
LOKI_AUTH = (LOKI_USERNAME, LOKI_PASSWORD) if LOKI_USERNAME and LOKI_PASSWORD else None


def _build_handlers():
    handlers = {
        "file": {
            "class": "logging.FileHandler",
            "formatter": "standard",
            "filename": str(LOG_DIR / "etl.log"),
            "level": "INFO",
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
        },
    }

    if LOKI_ENABLED:
        loki_handler = {
            "class": "logging_loki.LokiHandler",
            "formatter": "json",
            "url": LOKI_URL.rstrip("/") + "/loki/api/v1/push",
            "tags": {
                "app": "etl-weather",
                "job": JOB_NAME,
                "environment": ENVIRONMENT,
            },
            "version": "2",
            "level": "INFO",
        }
        if LOKI_AUTH:
            loki_handler["auth"] = LOKI_AUTH
        handlers["loki"] = loki_handler

    return handlers


def _build_logging_config():
    handlers = _build_handlers()
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            },
            "json": {
                "()": "src.helpers.logging_helpers.json_log_formatter.JSONFormatter",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": handlers,
        "root": {
            "level": "INFO",
            "handlers": list(handlers.keys()),
        },
    }


def setup_logging():
    logging.config.dictConfig(_build_logging_config())

    prefect_logger = logging.getLogger("prefect")
    prefect_logger.setLevel(logging.INFO)

    logger = logging.getLogger(__name__)
    if not LOKI_ENABLED:
        logger.warning("Loki logging DISABLED — LOKI_URL not set")
    elif LOKI_AUTH:
        logger.info("Loki logging enabled (with auth) — job=%s env=%s", JOB_NAME, ENVIRONMENT)
    else:
        logger.info("Loki logging enabled (no auth, local) — job=%s env=%s", JOB_NAME, ENVIRONMENT)















"""below i s old version, just keep for now if need backup restore"""
# import logging
# import logging.config
# import os
# from pathlib import Path
#
# LOG_DIR = Path("src/logs")
# LOG_DIR.mkdir(exist_ok=True)
#
# LOGGING_CONFIG = {
#     "version": 1,
#     "disable_existing_loggers": False,
#     "formatters": {
#         "standard": {
#             "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
#         },
#         "json": {
#             "()": "src.helpers.logging_helpers.json_log_formatter.JSONFormatter",
#             "datefmt": "%Y-%m-%d %H:%M:%S"
#         }
#     },
#     "handlers": {
#         "file": {
#             "class": "logging.FileHandler",
#             "formatter": "standard",
#             "filename": str(LOG_DIR / "etl.log"),
#             "level": "INFO",
#         },
#         "console": {
#             "class": "logging.StreamHandler",
#             "formatter": "standard",
#             "level": "INFO",
#         },
#         "loki": {
#             "class": "logging_loki.LokiHandler",
#             "formatter": "json",
#             "url": os.getenv("LOKI_URL", "http://localhost:3100") + "/loki/api/v1/push",
#             "tags": {"app": "etl-weather"},
#             "version": "2",
#             "level": "INFO",
#         }
#     },
#     "root": {
#         "level": "INFO",
#         "handlers": ["file", "console", "loki"],
#     },
# }
#
#
# def setup_logging():
#     logging.config.dictConfig(LOGGING_CONFIG)
#
#
#     prefect_logger = logging.getLogger("prefect")
#     prefect_logger.setLevel(logging.INFO)
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# # import logging
# # from pathlib import Path
# # from logging_loki import LokiHandler
# #
# # LOG_DIR = Path("src/logs")
# # LOG_DIR.mkdir(exist_ok=True)
# #
# # _INITIALIZED = False
# #
# #
# # def setup_logging():
# #     global _INITIALIZED
# #     if _INITIALIZED:
# #         return
# #
# #     formatter = logging.Formatter(
# #         "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
# #     )
# #
# #     file_handler = logging.FileHandler(LOG_DIR / "etl.log")
# #     file_handler.setLevel(logging.INFO)
# #     file_handler.setFormatter(formatter)
# #
# #     loki_handler = LokiHandler(
# #         url="http://loki:3100/loki/api/v1/push",
# #         tags={"app": "my-application"},
# #         version="2",
# #     )
# #     loki_handler.setLevel(logging.INFO)
# #     loki_handler.setFormatter(formatter)
# #
# #     # Root logger (everything)
# #     root = logging.getLogger()
# #     root.setLevel(logging.INFO)
# #     root.addHandler(file_handler)
# #     root.addHandler(loki_handler)
# #
# #     # Prefect logger (explicit)
# #     prefect_logger = logging.getLogger("prefect")
# #     prefect_logger.setLevel(logging.INFO)
# #     prefect_logger.addHandler(file_handler)
# #     prefect_logger.addHandler(loki_handler)
# #
# #     # 🔥 Prevent double logging
# #     prefect_logger.propagate = False
# #
# #     # Optional but HIGHLY recommended
# #     logging.getLogger("httpx").setLevel(logging.WARNING)
# #
# #     _INITIALIZED = True
# #
# #
