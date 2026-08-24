"""Unit tests for the Kalshi RSA-PSS signer and credential loading."""

from __future__ import annotations

import base64

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from kalshi_ws_collector import auth


@pytest.fixture(scope="module")
def keypair():
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return priv, priv.public_key()


def test_signed_message_format():
    # timestamp + METHOD(upper) + path, query stripped.
    msg = auth.signed_message("1700000000000", "get", "/trade-api/v2/markets?x=1")
    assert msg == "1700000000000GET/trade-api/v2/markets"


def test_auth_headers_signature_verifies(keypair):
    # PSS is randomized, so verify the signature rather than comparing bytes.
    priv, pub = keypair
    headers = auth.auth_headers("test-key", priv, "GET", "/trade-api/v2/markets?status=open")
    assert headers["KALSHI-ACCESS-KEY"] == "test-key"
    ts = headers["KALSHI-ACCESS-TIMESTAMP"]
    assert ts.isdigit()
    message = (ts + "GET" + "/trade-api/v2/markets").encode("utf-8")
    signature = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])
    pub.verify(
        signature,
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )  # raises InvalidSignature on mismatch


def test_ws_auth_headers_sign_ws_path(keypair):
    priv, pub = keypair
    headers = auth.ws_auth_headers("k", priv)
    ts = headers["KALSHI-ACCESS-TIMESTAMP"]
    message = (ts + "GET" + auth.KALSHI_WS_SIGN_PATH).encode("utf-8")
    signature = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])
    pub.verify(
        signature,
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )


def test_load_private_key_from_contents(keypair):
    priv, _ = keypair
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    loaded = auth.load_private_key(pem_contents=pem)
    assert loaded.key_size == 2048


def test_load_private_key_from_path(keypair, tmp_path):
    priv, _ = keypair
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pem_file = tmp_path / "key.pem"
    pem_file.write_bytes(pem)
    loaded = auth.load_private_key(pem_path=pem_file)
    assert loaded.key_size == 2048


def test_load_private_key_missing_raises(monkeypatch):
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PEM", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)
    with pytest.raises(RuntimeError):
        auth.load_private_key()


def test_creds_present_false_without_env(monkeypatch):
    monkeypatch.delenv("KALSHI_API_KEY_ID", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PEM", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)
    assert auth.creds_present() is False


def test_creds_present_true_with_inline_contents(monkeypatch):
    monkeypatch.setenv("KALSHI_API_KEY_ID", "k")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PEM", "-----BEGIN PRIVATE KEY-----")
    assert auth.creds_present() is True
