"""
Prometheus Remote Write client за Grafana Cloud Mimir.

Изпраща метрики директно през Remote Write протокола (snappy-compressed
protobuf), без нужда от Pushgateway. Използва ръчно, минимално protobuf
wire-format encoding за WriteRequest/TimeSeries/Label/Sample съобщенията -
това избягва зависимост от `protobuf` пакета и generated _pb2.py файлове
(които имат проблеми с version pinning - виж бележката по-долу).

Proto схема (от prometheus/prompb, само полетата които ползваме):

    message WriteRequest {
        repeated TimeSeries timeseries = 1;
    }
    message TimeSeries {
        repeated Label labels = 1;
        repeated Sample samples = 2;
    }
    message Label {
        string name = 1;
        string value = 2;
    }
    message Sample {
        double value = 1;
        int64 timestamp = 2;   // milliseconds since epoch
    }

Спецификация: https://prometheus.io/docs/specs/prw/remote_write_spec/
"""

import os
import struct
import time
import logging
from typing import Iterable, Sequence, Tuple

import requests
import snappy  # pip install python-snappy (изисква libsnappy-dev/brew snappy)

logger = logging.getLogger(__name__)

# ---- Конфигурация от env vars (вече setнати в Terraform за петте job-а) ----
PROMETHEUS_REMOTE_WRITE_URL = os.environ.get("PROMETHEUS_REMOTE_WRITE_URL")
PROMETHEUS_USERNAME = os.environ.get("PROMETHEUS_USERNAME")
PROMETHEUS_PASSWORD = os.environ.get("PROMETHEUS_PASSWORD")

REQUEST_TIMEOUT_SECONDS = 15  # аналогично на timeout fix-а в weather workers-ите

Label = Tuple[str, str]
Sample = Tuple[float, int]  # (value, timestamp_ms)
TimeSeriesInput = Tuple[Sequence[Label], Sequence[Sample]]


# --------------------------------------------------------------------------
# Ниско ниво: ръчен protobuf wire-format encoder
# --------------------------------------------------------------------------

def _encode_varint(value: int) -> bytes:
    """Unsigned LEB128 varint encoding (protobuf wire type 0)."""
    if value < 0:
        raise ValueError("Timestamp/length не може да е отрицателен за нашата схема")
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def _encode_tag(field_number: int, wire_type: int) -> bytes:
    return _encode_varint((field_number << 3) | wire_type)


def _encode_string_field(field_number: int, value: str) -> bytes:
    raw = value.encode("utf-8")
    return _encode_tag(field_number, 2) + _encode_varint(len(raw)) + raw


def _encode_double_field(field_number: int, value: float) -> bytes:
    return _encode_tag(field_number, 1) + struct.pack("<d", float(value))


def _encode_varint_field(field_number: int, value: int) -> bytes:
    return _encode_tag(field_number, 0) + _encode_varint(value)


def _encode_embedded(field_number: int, message_bytes: bytes) -> bytes:
    return _encode_tag(field_number, 2) + _encode_varint(len(message_bytes)) + message_bytes


def _encode_label(name: str, value: str) -> bytes:
    return _encode_string_field(1, name) + _encode_string_field(2, value)


def _encode_sample(value: float, timestamp_ms: int) -> bytes:
    return _encode_double_field(1, value) + _encode_varint_field(2, timestamp_ms)


def _encode_timeseries(labels: Sequence[Label], samples: Sequence[Sample]) -> bytes:
    buf = bytearray()
    for name, value in labels:
        buf += _encode_embedded(1, _encode_label(name, value))
    for value, timestamp_ms in samples:
        buf += _encode_embedded(2, _encode_sample(value, timestamp_ms))
    return bytes(buf)


def _encode_write_request(timeseries_list: Iterable[TimeSeriesInput]) -> bytes:
    buf = bytearray()
    for labels, samples in timeseries_list:
        buf += _encode_embedded(1, _encode_timeseries(labels, samples))
    return bytes(buf)


# --------------------------------------------------------------------------
# Високо ниво: публичен API
# --------------------------------------------------------------------------

def build_timeseries(
    metric_name: str,
    value: float,
    extra_labels: dict | None = None,
    timestamp_ms: int | None = None,
) -> TimeSeriesInput:
    """
    Помощна функция за построяване на една TimeSeries.

    Пример:
        ts = build_timeseries(
            "weather_etl_job_duration_seconds",
            42.5,
            extra_labels={"job": "hourly_ingest", "environment": "azure"},
        )
        push_metrics([ts])
    """
    labels = [("__name__", metric_name)]
    if extra_labels:
        labels.extend(sorted(extra_labels.items()))
    ts_ms = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    return labels, [(value, ts_ms)]


def push_metrics(timeseries_list: Sequence[TimeSeriesInput]) -> bool:
    """
    Изпраща списък от TimeSeries към Grafana Cloud Mimir чрез Remote Write.

    Връща True при успех (HTTP 2xx), False при неуспех - НЕ хвърля exception,
    за да не чупи главния ETL flow (аналогично на LokiQueueHandler подхода -
    метриките/логовете никога не трябва да бъдат critical path).
    """
    if not PROMETHEUS_REMOTE_WRITE_URL:
        logger.debug("PROMETHEUS_REMOTE_WRITE_URL не е зададен - remote write skip.")
        return False

    if not timeseries_list:
        return True

    try:
        payload = _encode_write_request(timeseries_list)
        compressed = snappy.compress(payload)

        auth = None
        if PROMETHEUS_USERNAME and PROMETHEUS_PASSWORD:
            auth = (PROMETHEUS_USERNAME, PROMETHEUS_PASSWORD)

        response = requests.post(
            PROMETHEUS_REMOTE_WRITE_URL,
            data=compressed,
            auth=auth,
            headers={
                "Content-Encoding": "snappy",
                "Content-Type": "application/x-protobuf",
                "X-Prometheus-Remote-Write-Version": "0.1.0",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        logger.debug("Remote write: изпратени %d timeseries.", len(timeseries_list))
        return True

    except requests.exceptions.RequestException as exc:
        logger.warning("Prometheus remote write push неуспешен: %s", exc)
        return False
    except Exception as exc:  # noqa: BLE001 - метриките никога не трябва да чупят job-а
        logger.warning("Неочаквана грешка при remote write encoding: %s", exc)
        return False