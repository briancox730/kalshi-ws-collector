"""Kalshi RSA-PSS request signing and credential loading.

Kalshi authenticates every REST request and the WebSocket handshake with three
headers derived from an RSA private key:

    KALSHI-ACCESS-KEY        the API key id
    KALSHI-ACCESS-TIMESTAMP  unix epoch milliseconds (as a string)
    KALSHI-ACCESS-SIGNATURE  base64( RSA-PSS-SHA256( timestamp + METHOD + path ) )

The signed ``path`` is the request path WITHOUT its query string; for the
WebSocket handshake it is the fixed ``/trade-api/ws/v2``.

Credentials are read from the environment:

    KALSHI_API_KEY_ID        required — the API key id
    KALSHI_PRIVATE_KEY_PEM   the RSA private-key PEM, inline (takes precedence), OR
    KALSHI_PRIVATE_KEY_PATH  path to the .pem file
    KALSHI_API_URL           REST base URL (optional override)
    KALSHI_WS_URL            WebSocket URL (optional override)

You do not generate the key pair yourself: Kalshi creates it for you on the API
keys page of your kalshi.com account. You receive an API key id and a one-time
download of the private-key .pem — point ``KALSHI_PRIVATE_KEY_PATH`` at that
file (or paste its contents into ``KALSHI_PRIVATE_KEY_PEM``).
"""

from __future__ import annotations

import base64
import os
import time
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

# --- endpoints -------------------------------------------------------------

KALSHI_API_BASE = os.getenv(
    "KALSHI_API_URL", "https://api.elections.kalshi.com/trade-api/v2"
)
KALSHI_WS_URL = os.getenv(
    "KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2"
)
KALSHI_WS_SIGN_PATH = "/trade-api/ws/v2"

# --- series map ------------------------------------------------------------

# Series ticker per asset for the 15-minute crypto up/down markets.
KALSHI_SERIES: dict[str, str] = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "SOL": "KXSOL15M",
    "XRP": "KXXRP15M",
}

# The assets we collect, in a stable order.
KALSHI_ASSETS: list[str] = list(KALSHI_SERIES)

# --- environment variable names --------------------------------------------

ENV_API_KEY_ID = "KALSHI_API_KEY_ID"
ENV_PRIVATE_KEY_PEM = "KALSHI_PRIVATE_KEY_PEM"
ENV_PRIVATE_KEY_PATH = "KALSHI_PRIVATE_KEY_PATH"


def load_private_key(
    pem_path: str | os.PathLike | None = None,
    pem_contents: str | None = None,
) -> RSAPrivateKey:
    """Load the RSA private key from inline contents or a PEM file.

    Resolution order: explicit ``pem_contents`` arg → ``KALSHI_PRIVATE_KEY_PEM``
    env → explicit ``pem_path`` arg → ``KALSHI_PRIVATE_KEY_PATH`` env. Inline
    contents win so containerized deploys can inject the key without a mounted
    file. Raises ``RuntimeError`` if no source is configured.
    """
    contents = pem_contents if pem_contents is not None else os.getenv(ENV_PRIVATE_KEY_PEM, "")
    if contents:
        data = contents.encode("utf-8")
    else:
        raw_path = pem_path or os.getenv(ENV_PRIVATE_KEY_PATH)
        if not raw_path:
            raise RuntimeError(
                f"no Kalshi private key configured: set {ENV_PRIVATE_KEY_PEM} "
                f"(inline PEM) or {ENV_PRIVATE_KEY_PATH} (path to the .pem file)"
            )
        path = Path(raw_path)
        if not path.exists():
            raise RuntimeError(f"Kalshi private key not found at {path}")
        data = path.read_bytes()
    key = serialization.load_pem_private_key(data, password=None, backend=default_backend())
    if not isinstance(key, RSAPrivateKey):
        raise TypeError("Kalshi private key must be an RSA key")
    return key


def sign(private_key: RSAPrivateKey, message: str) -> str:
    """Return base64( RSA-PSS-SHA256(message) ) — Kalshi's signature scheme.

    PSS is randomized (per-signature salt), so the output is non-deterministic;
    verify with the public key and identical PSS params, do not compare bytes.
    """
    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def _timestamp_ms() -> str:
    return str(int(time.time() * 1000))


def signed_message(timestamp: str, method: str, path: str) -> str:
    """The exact string Kalshi signs: ``timestamp + METHOD + path`` (no query)."""
    return timestamp + method.upper() + path.split("?", 1)[0]


def auth_headers(
    api_key_id: str, private_key: RSAPrivateKey, method: str, path: str
) -> dict[str, str]:
    """Signed headers for a REST request or the WS handshake.

    ``path`` is signed WITHOUT its query string. For the WS handshake pass
    ``method="GET"`` and ``path=KALSHI_WS_SIGN_PATH``. A fresh timestamp is
    generated on every call, so call this once per request / per (re)connect.
    """
    timestamp = _timestamp_ms()
    msg = signed_message(timestamp, method, path)
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": sign(private_key, msg),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


def ws_auth_headers(api_key_id: str, private_key: RSAPrivateKey) -> dict[str, str]:
    """Signed headers for the Kalshi WebSocket handshake."""
    return auth_headers(api_key_id, private_key, "GET", KALSHI_WS_SIGN_PATH)


def creds_present() -> bool:
    """True iff a usable API key id and a private-key source are configured.

    Lets a supervisor skip the collectors entirely on a machine without
    credentials instead of spinning them into a reconnect loop.
    """
    if not os.getenv(ENV_API_KEY_ID):
        return False
    if os.getenv(ENV_PRIVATE_KEY_PEM):
        return True
    path = os.getenv(ENV_PRIVATE_KEY_PATH)
    return bool(path) and Path(path).exists()
