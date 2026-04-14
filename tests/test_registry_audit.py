from __future__ import annotations

from arb.scripts.registry_audit import RegistrySnapshot, diff_snapshots


def test_diff_snapshots_detects_added_removed_market_ids() -> None:
    prev = RegistrySnapshot(
        generated_at="2026-01-01T00:00:00Z",
        total_markets=2,
        venue_counts={"felix": 1, "hl_native": 1},
        market_ids=["hl-perp:BTC", "hl-spot:@1"],
        symbols_by_venue={"felix": ["AAPL"], "hl_native": ["BTC"]},
    )
    cur = RegistrySnapshot(
        generated_at="2026-01-02T00:00:00Z",
        total_markets=3,
        venue_counts={"felix": 2, "hl_native": 1},
        market_ids=["hl-perp:BTC", "hl-spot:@1", "hl-spot:@2"],
        symbols_by_venue={"felix": ["AAPL", "MSFT"], "hl_native": ["BTC"]},
    )

    diff = diff_snapshots(prev, cur)
    assert diff["added_market_ids"] == ["hl-spot:@2"]
    assert diff["removed_market_ids"] == []
    assert diff["venue_count_delta"]["felix"] == 1
    assert diff["new_symbols_by_venue"]["felix"] == ["MSFT"]


def test_diff_snapshots_first_run_has_no_previous() -> None:
    cur = RegistrySnapshot(
        generated_at="2026-01-02T00:00:00Z",
        total_markets=1,
        venue_counts={"hl_native": 1},
        market_ids=["hl-perp:BTC"],
        symbols_by_venue={"hl_native": ["BTC"]},
    )
    diff = diff_snapshots(None, cur)
    assert diff["added_market_ids"] == ["hl-perp:BTC"]
    assert diff["removed_market_ids"] == []
    assert diff["venue_count_delta"]["hl_native"] == 1
