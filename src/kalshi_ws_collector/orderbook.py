"""Pure order-book mirroring for one Kalshi contract.

A Kalshi orderbook is two stacks of resting BUY orders: the ``yes`` stack and
the ``no`` stack. Both are sorted ascending by price, so the best (highest)
resting bid is the top of each stack. The two sides are linked by the identity

    yes_ask = 1 - top_no_bid        no_ask = 1 - top_yes_bid

because buying ``no`` at price *p* is economically selling ``yes`` at ``1 - p``
(prices are normalised to dollars in [0, 1]).

The exchange sends one ``orderbook_snapshot`` establishing the full book, then a
stream of ``orderbook_delta`` messages, each a signed size change at a single
(side, price). This module keeps the book as ``{side: {price_key: size}}`` and
exposes pure functions to apply snapshots/deltas and read the top of book — no
network, no I/O, so the mirroring logic is fully unit-testable.
"""

from __future__ import annotations

import json

from kalshi_ws_collector.parsing import coerce_price

# A book is {"yes": {price_key: size}, "no": {price_key: size}}.
Stack = dict[str, float]
Book = dict[str, Stack]


def empty_book() -> Book:
    """A fresh, empty two-sided book."""
    return {"yes": {}, "no": {}}


def ladder_from_snapshot(msg: dict, side: str) -> Stack:
    """Build a ``{price_key: size}`` stack for one side of a snapshot.

    Accepts the fixed-point dollar variant (``yes_dollars_fp``), the dollar
    variant (``yes_dollars``), or the cent variant (``yes``). Each is a list of
    ``[price, size]`` pairs. Prices are canonicalised to a 4-dp dollar string
    key so snapshot and delta representations match regardless of variant.
    """
    rows = msg.get(f"{side}_dollars_fp") or msg.get(f"{side}_dollars") or msg.get(side) or []
    stack: Stack = {}
    for row in rows:
        try:
            price = coerce_price(row[0])
            size = float(row[1])
        except (IndexError, TypeError, ValueError):
            continue
        if price is None or size <= 0:
            continue
        stack[f"{price:.4f}"] = size
    return stack


def apply_snapshot(book: Book, msg: dict) -> None:
    """Replace both sides of ``book`` from an ``orderbook_snapshot`` message."""
    book["yes"] = ladder_from_snapshot(msg, "yes")
    book["no"] = ladder_from_snapshot(msg, "no")


def apply_delta(book: Book, msg: dict) -> None:
    """Apply one ``orderbook_delta`` message to ``book`` in place.

    A delta is a signed size change at a single (side, price). A resulting size
    at or below zero removes the level.
    """
    side = msg.get("side")
    if side not in ("yes", "no"):
        return
    price = coerce_price(msg.get("price_dollars", msg.get("price")))
    if price is None:
        return
    try:
        delta = float(msg.get("delta_fp", msg.get("delta", 0)))
    except (TypeError, ValueError):
        return
    stack = book[side]
    key = f"{price:.4f}"
    new_size = stack.get(key, 0.0) + delta
    if new_size <= 1e-9:
        stack.pop(key, None)
    else:
        stack[key] = new_size


def top_bid(stack: Stack) -> float | None:
    """Highest (best) resting bid price in a stack, or None if empty."""
    if not stack:
        return None
    return max(float(k) for k in stack)


def levels_json(stack: Stack) -> str:
    """Serialise a stack to ``[[price, size], ...]`` JSON, ascending by price."""
    levels = sorted(((float(k), v) for k, v in stack.items()), key=lambda kv: kv[0])
    return json.dumps([[p, s] for p, s in levels])
