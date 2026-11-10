"""Normalized reward terms accumulated over simulated event intervals."""

from dataclasses import dataclass, asdict
import math


@dataclass(frozen=True)
class RewardConfig:
    throughput: float = 1.0
    queue: float = 0.1
    slowdown: float = 0.1
    fragmentation: float = 0.01
    sla: float = 1.0
    invalid: float = 1.0
    time_scale: float = 10.0
    slowdown_clip: float = 100.0

    def validate(self):
        for key, value in asdict(self).items():
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"invalid reward setting {key}")
        if self.time_scale == 0 or self.slowdown_clip < 1:
            raise ValueError("reward scales must be positive")

    def weighted(self, terms):
        return sum(terms[key] * getattr(self, key) for key in terms)


def empty_terms():
    return dict.fromkeys(("throughput", "queue", "slowdown", "fragmentation", "sla", "invalid"), 0.0)
