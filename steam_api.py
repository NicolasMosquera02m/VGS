"""Small client for Steam's public Store endpoints.

The Store endpoints are public and do not require an API key. Calls are
intentionally limited to search and individual app details, with Streamlit
handling the short-lived cache at the UI boundary.
"""

from __future__ import annotations

import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from model.rentability_model import is_known_aaa_title

STEAM_STORE_URL = "https://store.steampowered.com/api"


class SteamApiError(RuntimeError):
    """Raised when Steam returns an invalid or unsuccessful response."""


def _get_json(path: str, params: dict[str, str | int]) -> dict:
    url = f"{STEAM_STORE_URL}/{path}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "VGS-Rentability/1.0"})
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, ValueError) as exc:
        raise SteamApiError("No fue posible consultar Steam en este momento.") from exc
    if not isinstance(payload, dict):
        raise SteamApiError("Steam devolvió una respuesta inesperada.")
    return payload


def search_games(term: str, country: str = "co", language: str = "spanish") -> list[dict]:
    if not term.strip():
        return []
    payload = _get_json(
        "storesearch/",
        {"term": term.strip(), "cc": country, "l": language, "infinite": 1},
    )
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise SteamApiError("Steam no devolvió resultados de búsqueda válidos.")
    return [item for item in items if item.get("type") == "app" and item.get("id")]


def get_game_details(app_id: int, country: str = "co", language: str = "spanish") -> dict:
    payload = _get_json(
        "appdetails",
        {"appids": int(app_id), "cc": country, "l": language},
    )
    result = payload.get(str(app_id))
    if not isinstance(result, dict) or not result.get("success"):
        raise SteamApiError("Steam no encontró los detalles de ese juego.")
    data = result.get("data")
    if not isinstance(data, dict):
        raise SteamApiError("Steam devolvió detalles incompletos.")
    return data


def normalize_game(data: dict) -> dict:
    price = data.get("price_overview") or {}
    return {
        "app_id": data.get("steam_appid"),
        "name": data.get("name", "Sin nombre"),
        "release_date": (data.get("release_date") or {}).get("date", "No disponible"),
        "genres": ", ".join(item.get("description", "") for item in data.get("genres", [])),
        "platforms": ", ".join(
            name for name, enabled in (data.get("platforms") or {}).items() if enabled
        ),
        "is_free": bool(data.get("is_free")),
        "price": price.get("final_formatted", "Gratis" if data.get("is_free") else "No disponible"),
        "metacritic": (data.get("metacritic") or {}).get("score", "No disponible"),
        "developers": ", ".join(data.get("developers") or []) or "No disponible",
        "publisher": ", ".join(data.get("publishers") or []) or "No disponible",
        "header_image": data.get("header_image"),
        "short_description": data.get("short_description", ""),
        "store_url": f"https://store.steampowered.com/app/{data.get('steam_appid')}",
    }


def is_excluded_large_game(data: dict) -> bool:
    """Apply the known AAA franchise part of the investment filter."""
    return is_known_aaa_title(data.get("name", ""))
