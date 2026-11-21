from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_url: str
    raw_data_dir: Path
    http_timeout_seconds: float = 30
    http_max_attempts: int = 4
    price_move_alert_percent: float = 0.3
    mfapi_base_url: str = "https://api.mfapi.in"
    nse_eod_url_template: str | None = None
    nse_index_url_template: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL must be set")
        attempts = int(os.getenv("HTTP_MAX_ATTEMPTS", "4"))
        timeout = float(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
        move_threshold = float(os.getenv("PRICE_MOVE_ALERT_PERCENT", "0.30"))
        if attempts < 1:
            raise ValueError("HTTP_MAX_ATTEMPTS must be at least 1")
        if timeout <= 0:
            raise ValueError("HTTP_TIMEOUT_SECONDS must be positive")
        if move_threshold <= 0:
            raise ValueError("PRICE_MOVE_ALERT_PERCENT must be positive")
        return cls(
            database_url=database_url,
            raw_data_dir=Path(os.getenv("RAW_DATA_DIR", "./data/raw")),
            http_timeout_seconds=timeout,
            http_max_attempts=attempts,
            price_move_alert_percent=move_threshold,
            mfapi_base_url=os.getenv("MFAPI_BASE_URL", "https://api.mfapi.in"),
            nse_eod_url_template=os.getenv("NSE_EOD_URL_TEMPLATE"),
            nse_index_url_template=os.getenv("NSE_INDEX_URL_TEMPLATE"),
        )
