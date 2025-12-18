from dataclasses import dataclass
from typing import List


DEFAULT_STRATEGIES = [
    "golden_cross",
    "rsi_momentum",
    "breakout_52w",
    "volume_surge",
]


@dataclass
class ScanConfig:
    universe: str = "sp500"  # or path to file
    strategies: List[str] = None
    lookback_days: int = 252

    def effective_strategies(self) -> List[str]:
        return self.strategies or DEFAULT_STRATEGIES
