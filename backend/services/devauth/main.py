"""A minimal OIDC issuer for development, CI and load tests (ADR-0016).

It exists because the application verifies a signature against a JWKS URL and
nothing more, so the issuer is swappable — and because a load test that spends
its time inside a real identity provider's interactive flows is measuring the
wrong system. POST /token mints a token for any subject, instantly.

NOT for production. The signing key is committed to this repository.
"""

from __future__ import annotations

import pathlib
import time
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from fastapi import FastAPI
from jwt.utils import base64url_encode
from pydantic import BaseModel

KEY_PATH = pathlib.Path(__file__).with_name("dev-only-insecure-signing-key.pem")
KEY_ID = "dev-key-1"
ISSUER = "http://devauth:8080"
AUDIENCE = "video-pipeline"

_private_key = serialization.load_pem_private_key(KEY_PATH.read_bytes(), password=None)
_public_numbers = _private_key.public_key().public_numbers()

app = FastAPI(title="dev OIDC issuer", docs_url="/docs")


def _b64(value: int) -> str:
    length = (value.bit_length() + 7) // 8
    return base64url_encode(value.to_bytes(length, "big")).decode()


@app.get("/.well-known/openid-configuration")
def discovery() -> dict[str, Any]:
    return {
        "issuer": ISSUER,
        "jwks_uri": f"{ISSUER}/jwks.json",
        "token_endpoint": f"{ISSUER}/token",
        "id_token_signing_alg_values_supported": ["RS256"],
    }


@app.get("/jwks.json")
def jwks() -> dict[str, Any]:
    return {
        "keys": [
            {
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": KEY_ID,
                "n": _b64(_public_numbers.n),
                "e": _b64(_public_numbers.e),
            }
        ]
    }


class TokenRequest(BaseModel):
    sub: str
    expires_in: int = 3600
    audience: str = AUDIENCE


@app.post("/token")
def issue_token(request: TokenRequest) -> dict[str, Any]:
    """Mint a token for any subject. The whole point is that this is trivial."""
    now = int(time.time())
    token = jwt.encode(
        {
            "sub": request.sub,
            "iss": ISSUER,
            "aud": request.audience,
            "iat": now,
            "exp": now + request.expires_in,
        },
        _private_key,
        algorithm="RS256",
        headers={"kid": KEY_ID},
    )
    return {"access_token": token, "token_type": "Bearer", "expires_in": request.expires_in}


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "alive"}
