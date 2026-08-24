"""Unit tests for the pure order-book mirror (snapshot/delta/top-of-book)."""

from __future__ import annotations

import json

from kalshi_ws_collector.orderbook import (
    apply_delta,
    apply_snapshot,
    empty_book,
    ladder_from_snapshot,
    levels_json,
    top_bid,
)


def test_ladder_from_snapshot_canonicalises_prices():
    msg = {"yes_dollars_fp": [["0.40", "100"], ["0.45", "60"]]}
    stack = ladder_from_snapshot(msg, "yes")
    assert stack == {"0.4000": 100.0, "0.4500": 60.0}


def test_ladder_drops_nonpositive_and_bad_rows():
    msg = {"yes": [["0.40", "0"], ["0.45", "-5"], ["bad"], ["0.50", "10"]]}
    assert ladder_from_snapshot(msg, "yes") == {"0.5000": 10.0}


def test_snapshot_top_of_book():
    book = empty_book()
    apply_snapshot(book, {
        "yes_dollars_fp": [["0.40", "100"], ["0.45", "60"]],
        "no_dollars_fp": [["0.50", "30"], ["0.55", "20"]],
    })
    # Best bid is the highest price in each ascending stack.
    assert top_bid(book["yes"]) == 0.45
    assert top_bid(book["no"]) == 0.55


def test_delta_raises_new_best_bid():
    book = empty_book()
    apply_snapshot(book, {"yes_dollars_fp": [["0.45", "60"]], "no_dollars_fp": []})
    apply_delta(book, {"side": "yes", "price_dollars": "0.48", "delta_fp": "25"})
    assert top_bid(book["yes"]) == 0.48


def test_delta_removes_level_when_size_hits_zero():
    book = empty_book()
    apply_snapshot(book, {
        "yes_dollars_fp": [["0.45", "60"], ["0.40", "10"]], "no_dollars_fp": [],
    })
    apply_delta(book, {"side": "yes", "price_dollars": "0.45", "delta_fp": "-60"})
    # 0.45 removed; next best is 0.40.
    assert "0.4500" not in book["yes"]
    assert top_bid(book["yes"]) == 0.40


def test_delta_accumulates_size():
    book = empty_book()
    apply_snapshot(book, {"yes_dollars_fp": [["0.45", "60"]], "no_dollars_fp": []})
    apply_delta(book, {"side": "yes", "price_dollars": "0.45", "delta_fp": "15"})
    assert book["yes"]["0.4500"] == 75.0


def test_delta_ignores_unknown_side():
    book = empty_book()
    apply_snapshot(book, {"yes_dollars_fp": [["0.45", "60"]], "no_dollars_fp": []})
    apply_delta(book, {"side": "maybe", "price_dollars": "0.45", "delta_fp": "15"})
    assert book["yes"]["0.4500"] == 60.0


def test_top_bid_empty_is_none():
    assert top_bid({}) is None


def test_levels_json_sorted_ascending():
    stack = {"0.4500": 60.0, "0.4000": 100.0}
    assert json.loads(levels_json(stack)) == [[0.40, 100.0], [0.45, 60.0]]
