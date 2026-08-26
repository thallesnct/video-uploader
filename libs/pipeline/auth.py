"""Token verification (ADR-0016).

Verification, not decoding. The distinction matters: `jwt.decode(..., options=
{"verify_signature": False})` reads exactly the same claims and accepts a token
anyone could have written. Everything here goes through the JWKS.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pipeline.settings import AuthSettings, auth_settings


class AuthError(Exception):
    """Token missing, malformed, unverifiable, or for someone else."""


@dataclass(frozen=True)
class Principal:
    """A verified caller. owner_id is the tenancy key everything else hangs off."""

    owner_id: str
    claims: dict[str, Any]


class TokenVerifier:
    def __init__(self, settings: AuthSettings | None = None, jwk_client: Any = None) -> None:
        self._settings = settings or auth_settings()
        self._jwk_client = jwk_client

    @property
    def jwk_client(self) -> Any:
        if self._jwk_client is None:
            from jwt import PyJWKClient

            # Caches keys and refetches when an unknown kid appears, so issuer
            # key rotation does not require a redeploy.
            self._jwk_client = PyJWKClient(self._settings.jwks_url, cache_keys=True)
        return self._jwk_client

    def verify(self, token: str) -> Principal:
        import jwt

        try:
            signing_key = self.jwk_client.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                audience=self._settings.audience,
                issuer=self._settings.issuer,
                options={"require": ["exp", "iat", "sub", "aud", "iss"]},
            )
        except Exception as exc:  # PyJWT raises a family of these
            raise AuthError(f"token rejected: {exc}") from exc

        subject = claims.get("sub")
        if not subject:
            raise AuthError("token has no sub claim")
        return Principal(owner_id=str(subject), claims=claims)


def bearer_token(authorization: str | None) -> str:
    """Pull the token out of an Authorization header."""
    if not authorization:
        raise AuthError("missing Authorization header")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise AuthError("expected an 'Authorization: Bearer <token>' header")
    return token
