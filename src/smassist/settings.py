from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import DEFAULT_STRATEGIES
from .exceptions import ConfigError

try:
    import tomllib  # Python 3.11+
except Exception:  # pragma: no cover
    tomllib = None  # type: ignore


@dataclass
class ScanSettings:
    universe: str = "sp500"
    strategies: List[str] = field(default_factory=lambda: list(DEFAULT_STRATEGIES))
    lookback_days: int = 252
    excel_path: str = "data/Top500_Sample_Strategy_Playbook.xlsx"
    aggregate: str = "best"  # best|sum


@dataclass
class AppSettings:
    log_level: str = "INFO"
    scan: ScanSettings = field(default_factory=ScanSettings)

    # Reserved for future: per-strategy thresholds/parameters.
    strategy_params: Dict[str, Dict[str, Any]] = field(default_factory=dict)


def _parse_strategies(val: str) -> List[str]:
    items = [v.strip() for v in val.split(",")]
    return [v for v in items if v]


def _load_toml(path: Path) -> Dict[str, Any]:
    if tomllib is None:
        raise ConfigError("tomllib not available; requires Python 3.11+")
    try:
        data = path.read_bytes()
        return tomllib.loads(data.decode("utf-8"))
    except FileNotFoundError:
        return {}
    except Exception as e:
        raise ConfigError(f"Failed to parse TOML config: {path}: {e}") from e


def load_settings(config_path: Optional[str] = None) -> AppSettings:
    """Load settings from TOML + environment variables.

    Precedence (lowest → highest): defaults → TOML → env overrides.

    Env vars supported:
    - SMASSIST_LOG_LEVEL
    - SMASSIST_UNIVERSE
    - SMASSIST_STRATEGIES (comma-separated)
    - SMASSIST_LOOKBACK_DAYS
    - SMASSIST_EXCEL
    - SMASSIST_AGGREGATE
    """
    settings = AppSettings()

    if config_path:
        cfg = _load_toml(Path(config_path))
    else:
        # Optional default config file if present.
        cfg = _load_toml(Path("settings.toml"))

    app = cfg.get("app", {}) if isinstance(cfg, dict) else {}
    scan = cfg.get("scan", {}) if isinstance(cfg, dict) else {}
    strat = cfg.get("strategy", {}) if isinstance(cfg, dict) else {}

    if isinstance(app, dict) and app.get("log_level"):
        settings.log_level = str(app["log_level"]).upper()

    if isinstance(scan, dict):
        if scan.get("universe"):
            settings.scan.universe = str(scan["universe"])
        if scan.get("lookback_days") is not None:
            try:
                settings.scan.lookback_days = int(scan["lookback_days"])
            except Exception:
                raise ConfigError("scan.lookback_days must be an integer")
        if scan.get("excel"):
            settings.scan.excel_path = str(scan["excel"])
        if scan.get("aggregate"):
            settings.scan.aggregate = str(scan["aggregate"]).lower()
        if scan.get("strategies"):
            if not isinstance(scan["strategies"], list):
                raise ConfigError("scan.strategies must be a list")
            settings.scan.strategies = [str(s) for s in scan["strategies"]]

    if isinstance(strat, dict):
        settings.strategy_params = {k: (v if isinstance(v, dict) else {}) for k, v in strat.items()}

    # Env overrides
    if os.getenv("SMASSIST_LOG_LEVEL"):
        settings.log_level = os.getenv("SMASSIST_LOG_LEVEL", "INFO").upper()
    if os.getenv("SMASSIST_UNIVERSE"):
        settings.scan.universe = os.getenv("SMASSIST_UNIVERSE", settings.scan.universe)
    if os.getenv("SMASSIST_STRATEGIES"):
        settings.scan.strategies = _parse_strategies(os.getenv("SMASSIST_STRATEGIES", ""))
    if os.getenv("SMASSIST_LOOKBACK_DAYS"):
        try:
            settings.scan.lookback_days = int(os.getenv("SMASSIST_LOOKBACK_DAYS", "252"))
        except Exception:
            raise ConfigError("SMASSIST_LOOKBACK_DAYS must be an integer")
    if os.getenv("SMASSIST_EXCEL"):
        settings.scan.excel_path = os.getenv("SMASSIST_EXCEL", settings.scan.excel_path)
    if os.getenv("SMASSIST_AGGREGATE"):
        settings.scan.aggregate = os.getenv("SMASSIST_AGGREGATE", settings.scan.aggregate).lower()

    if settings.scan.aggregate not in ("best", "sum"):
        raise ConfigError("scan.aggregate must be 'best' or 'sum'")

    return settings
