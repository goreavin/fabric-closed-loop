from __future__ import annotations

import json
import os
import pathlib
import time

import msal

# Default path where the Fabric CLI stores its MSAL token cache.
# Adjust this to match your environment (e.g. ~/.config/fab/cache.bin).
DEFAULT_CACHE_PATH = pathlib.Path.home() / ".config" / "fab" / "cache.bin"
FABRIC_AUDIENCE_HINT = "api.fabric.microsoft.com"
EXPIRY_BUFFER_SECONDS = 60


class AuthError(RuntimeError):
    pass


def get_fabric_token(cache_path: os.PathLike | str | None = None) -> str:
    path = pathlib.Path(cache_path) if cache_path is not None else DEFAULT_CACHE_PATH
    if not path.exists():
        raise AuthError(
            f"fab MSAL cache not found at {path}. Run `fab auth login`."
        )

    cache = msal.SerializableTokenCache()
    cache.deserialize(path.read_text())
    access_tokens = json.loads(cache.serialize()).get("AccessToken", {})

    now = int(time.time())
    for entry in access_tokens.values():
        if FABRIC_AUDIENCE_HINT not in entry.get("target", ""):
            continue
        expires_on = int(entry.get("expires_on", "0"))
        if expires_on - now <= EXPIRY_BUFFER_SECONDS:
            raise AuthError(
                "Fabric access token is expired or about to expire. "
                "Run `fab auth login`."
            )
        return entry["secret"]

    raise AuthError(
        "No Fabric API access token found in fab MSAL cache. "
        "Run `fab auth login`."
    )
