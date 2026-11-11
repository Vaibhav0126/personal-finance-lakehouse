"""Tests for the champion/challenger rule (pure Python, no Spark needed)."""
import pytest

from ml.promotion import should_promote


def test_promotes_when_no_champion():
    assert should_promote(0.70, None).promote


def test_promotes_when_better():
    assert should_promote(0.85, 0.80).promote


def test_keeps_champion_on_tie():
    assert not should_promote(0.80, 0.80).promote


def test_keeps_champion_when_worse():
    decision = should_promote(0.75, 0.80)
    assert not decision.promote and "does not beat" in decision.reason


def test_respects_min_improvement():
    assert not should_promote(0.81, 0.80, min_improvement=0.02).promote
    assert should_promote(0.83, 0.80, min_improvement=0.02).promote


@pytest.mark.parametrize("bad", [-0.1, 1.5])
def test_rejects_out_of_range_accuracy(bad):
    with pytest.raises(ValueError):
        should_promote(bad, 0.5)
