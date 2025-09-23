# dags/common/defaults.py
import os
from datetime import datetime, timedelta
from functools import lru_cache

def _to_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default

def _to_bool(name: str, default: bool) -> bool:
    val = str(os.getenv(name, str(default))).strip().lower()
    return val in {"1", "true", "t", "yes", "y"}

def _to_minutes(name: str, default: int) -> timedelta:
    return timedelta(minutes=_to_int(name, default))

def _to_list(name: str, default_csv: str) -> list[str]:
    raw = os.getenv(name, default_csv)
    return [x.strip() for x in raw.split(",") if x.strip()]

def _to_date(name: str, default: str) -> datetime:
    """
    Parses YYYY-MM-DD from env. Airflow recommends a static start_date in the past.
    """
    s = os.getenv(name, default)
    return datetime.strptime(s, "%Y-%m-%d")

@lru_cache(maxsize=1)
def default_args() -> dict:
    """
    Build once, reuse in every DAG file.
    Use env vars if present; otherwise fall back to safe defaults.
    """
    return {
        "owner": os.getenv("AIRFLOW_OWNER", "airflow"),
        "depends_on_past": _to_bool("AIRFLOW_DEPENDS_ON_PAST", False),
        "email": _to_list("ALERT_EMAILS", "default@example.com"),
        "email_on_failure": _to_bool("AIRFLOW_EMAIL_ON_FAILURE", True),
        "email_on_retry": _to_bool("AIRFLOW_EMAIL_ON_RETRY", False),
        "retries": _to_int("AIRFLOW_RETRIES", 1),
        "retry_delay": _to_minutes("AIRFLOW_RETRY_DELAY_MINUTES", 5),
        # keep static date (not timezone-aware) unless you specifically need tz
        "start_date": _to_date("AIRFLOW_START_DATE", "2025-09-20"),
    }
