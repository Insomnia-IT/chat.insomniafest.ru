import asyncio
import importlib
import json
import pathlib
import sys

import httpx


BOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOT_DIR))


def load_bot_module(monkeypatch):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("SYNAPSE_REGISTRATION_SHARED_SECRET", "test-secret")
    monkeypatch.setenv("GRIST_API_KEY", "test-grist-key")

    if "bot" in sys.modules:
        del sys.modules["bot"]

    return importlib.import_module("bot")


def build_async_client_factory(real_async_client, transport):
    def _factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_async_client(*args, **kwargs)

    return _factory


def test_fetch_grist_records_contract(monkeypatch):
    bot = load_bot_module(monkeypatch)

    called = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        called["count"] += 1
        assert request.method == "GET"
        assert request.url.path.endswith("/tables/Participations/records")
        assert request.headers.get("Authorization") == "Bearer test-grist-key"

        filter_value = request.url.params.get("filter")
        assert '"year":[2026]' in filter_value
        assert '"status_code":["PLANNED","STARTED","COMPLETE"]' in filter_value

        return httpx.Response(
            200,
            json={
                "records": [
                    {
                        "id": 6178,
                        "fields": {
                            "Telegram2": "@test_member",
                            "team": 2,
                            "role_code": "ORGANIZER",
                        },
                    }
                ]
            },
        )

    transport = httpx.MockTransport(handler)
    real_async_client = bot.httpx.AsyncClient
    monkeypatch.setattr(
        bot.httpx,
        "AsyncClient",
        build_async_client_factory(real_async_client, transport),
    )

    records = asyncio.run(bot.fetch_grist_records_via_records_api())

    assert called["count"] == 1
    assert records[0]["id"] == 6178
    assert records[0]["fields"]["role_code"] == "ORGANIZER"


def test_register_synapse_user_http_flow(monkeypatch):
    bot = load_bot_module(monkeypatch)

    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))

        if request.method == "GET" and request.url.path.endswith("/_synapse/admin/v1/register"):
            return httpx.Response(200, json={"nonce": "nonce-123"})

        if request.method == "POST" and request.url.path.endswith("/_synapse/admin/v1/register"):
            payload = json.loads(request.content.decode("utf-8"))
            assert payload["username"] == "test_user"
            assert payload["password"] == "p@ss"
            assert payload["admin"] is False
            assert isinstance(payload["mac"], str)
            assert len(payload["mac"]) == 40
            return httpx.Response(200, json={"user_id": "@test_user:insomniafest.ru"})

        return httpx.Response(404, text="not found")

    transport = httpx.MockTransport(handler)
    real_async_client = bot.httpx.AsyncClient
    monkeypatch.setattr(
        bot.httpx,
        "AsyncClient",
        build_async_client_factory(real_async_client, transport),
    )

    ok, error = asyncio.run(bot.register_synapse_user("test_user", "p@ss"))

    assert ok is True
    assert error is None
    assert calls == [
        ("GET", "/_synapse/admin/v1/register"),
        ("POST", "/_synapse/admin/v1/register"),
    ]


def test_create_team_room_retries_without_alias(monkeypatch):
    bot = load_bot_module(monkeypatch)
    bot.SYNAPSE_ADMIN_ACCESS_TOKEN = "admin-token"

    requests_payloads = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "POST" or not request.url.path.endswith("/_matrix/client/v3/createRoom"):
            return httpx.Response(404, text="not found")

        payload = json.loads(request.content.decode("utf-8"))
        requests_payloads.append(payload)

        if len(requests_payloads) == 1:
            assert payload.get("room_alias_name") == "team-2"
            return httpx.Response(400, json={"errcode": "M_ROOM_IN_USE"})

        assert "room_alias_name" not in payload
        return httpx.Response(200, json={"room_id": "!team2:insomniafest.ru"})

    transport = httpx.MockTransport(handler)
    real_async_client = bot.httpx.AsyncClient
    monkeypatch.setattr(
        bot.httpx,
        "AsyncClient",
        build_async_client_factory(real_async_client, transport),
    )

    room_id = asyncio.run(bot.create_team_room(2, "2026.GR(Организатор)"))

    assert room_id == "!team2:insomniafest.ru"
    assert len(requests_payloads) == 2


def test_join_user_to_rooms_partial_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)
    bot.SYNAPSE_ADMIN_ACCESS_TOKEN = "admin-token"

    joined_paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "POST" or "/_synapse/admin/v1/join/" not in request.url.path:
            return httpx.Response(404, text="not found")

        joined_paths.append(request.url.path)
        payload = json.loads(request.content.decode("utf-8"))
        assert payload["user_id"] == "@test_user:insomniafest.ru"

        if len(joined_paths) == 1:
            return httpx.Response(200, json={})
        return httpx.Response(403, json={"errcode": "M_FORBIDDEN"})

    transport = httpx.MockTransport(handler)
    real_async_client = bot.httpx.AsyncClient
    monkeypatch.setattr(
        bot.httpx,
        "AsyncClient",
        build_async_client_factory(real_async_client, transport),
    )

    ok, failed = asyncio.run(
        bot.join_user_to_rooms("test_user", ["#ok:insomniafest.ru", "#fail:insomniafest.ru"])
    )

    assert ok is False
    assert failed == ["#fail:insomniafest.ru"]
    assert len(joined_paths) == 2
