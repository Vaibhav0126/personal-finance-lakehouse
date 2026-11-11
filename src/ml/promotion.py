"""
Champion/challenger promotion rule, kept free of MLflow and Spark so it can be
unit-tested anywhere.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PromotionDecision:
    promote: bool
    reason: str


def should_promote(
    challenger_accuracy: float,
    champion_accuracy: Optional[float],
    min_improvement: float = 0.0,
) -> PromotionDecision:
    """Decide whether a newly trained model replaces the current champion.

    Both accuracies must be measured on the same held-out test set; comparing
    against the champion's originally logged metric would be unfair because
    the data has grown since it was trained.

    * No champion yet  -> promote (bootstrap).
    * Otherwise promote only if the challenger beats the champion by more than
      `min_improvement`. A tie keeps the champion, avoiding churn.
    """
    if not 0.0 <= challenger_accuracy <= 1.0:
        raise ValueError(f"challenger_accuracy out of range: {challenger_accuracy}")
    if champion_accuracy is None:
        return PromotionDecision(True, "no existing champion")
    if not 0.0 <= champion_accuracy <= 1.0:
        raise ValueError(f"champion_accuracy out of range: {champion_accuracy}")

    gain = challenger_accuracy - champion_accuracy
    if gain > min_improvement:
        return PromotionDecision(
            True, f"challenger {challenger_accuracy:.3f} beats champion {champion_accuracy:.3f} by {gain:+.3f}"
        )
    return PromotionDecision(
        False,
        f"challenger {challenger_accuracy:.3f} does not beat champion "
        f"{champion_accuracy:.3f} by more than {min_improvement:.3f} (gain {gain:+.3f})",
    )
