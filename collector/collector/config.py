"""Configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    """Application configuration."""

    aircraft_url: str
    poll_interval: float
    session_timeout: float
    database_url: str

    @classmethod
    def from_env(cls) -> Config:
        """Load configuration from environment variables."""
        load_dotenv()
        return cls(
            aircraft_url=os.environ.get(
                "ADSB_URL",
                "http://192.168.0.111/tar1090/data/aircraft.json",
            ),
            poll_interval=float(os.environ.get("POLL_INTERVAL", "5")),
            session_timeout=float(os.environ.get("SESSION_TIMEOUT", "300")),
            database_url=os.environ["DATABASE_URL"],
        )
