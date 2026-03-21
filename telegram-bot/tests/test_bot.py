import asyncio
import importlib
import pathlib
import sys


BOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOT_DIR))


class FakeResponse:
    def __init__(self, status_code, json_data=None, text=""):
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else {}
        self.text = text

    def json(self):
        return self._json_data


class DummyMessage:
    def __init__(self):
        self.sent = []

    async def reply_text(self, text, parse_mode=None):
        self.sent.append({"text": text, "parse_mode": parse_mode})


class DummyUser:
    def __init__(self, user_id=1, username="alice"):
        self.id = user_id
        self.username = username


class DummyChat:
    def __init__(self, chat_id=1):
        self.id = chat_id


class DummyUpdate:
    def __init__(self, user_id=1, username="alice", chat_id=1):
        self.effective_user = DummyUser(user_id=user_id, username=username)
        self.message = DummyMessage()
        self.effective_chat = DummyChat(chat_id=chat_id)


class DummyBot:
    def __init__(self):
        self.sent = []

    async def send_message(self, chat_id, text):
        self.sent.append({"chat_id": chat_id, "text": text})


class DummyContext:
    def __init__(self, error=None, args=None):
        self.bot = DummyBot()
        self.error = error
        self.args = args if args is not None else []


def load_bot_module(monkeypatch):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("SYNAPSE_REGISTRATION_SHARED_SECRET", "test-secret")
    monkeypatch.setenv("GRIST_API_KEY", "test-grist-key")

    if "bot" in sys.modules:
        del sys.modules["bot"]

    return importlib.import_module("bot")


def test_normalize_telegram_handle(monkeypatch):
    bot = load_bot_module(monkeypatch)

    assert bot.normalize_telegram_handle("@UserName") == "username"
    assert bot.normalize_telegram_handle("  @MixedCase  ") == "mixedcase"
    assert bot.normalize_telegram_handle(["", " @ArrayHandle "]) == "arrayhandle"


def test_parse_grist_ref_id(monkeypatch):
    bot = load_bot_module(monkeypatch)

    assert bot.parse_grist_ref_id(2) == 2
    assert bot.parse_grist_ref_id(" 2 ") == 2
    assert bot.parse_grist_ref_id([2, "x"]) == 2
    assert bot.parse_grist_ref_id([]) is None
    assert bot.parse_grist_ref_id("") is None


def test_get_team_name_fallback(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name[7] = "Точка сборки"

    assert bot.get_team_name(7) == "Точка сборки"
    assert bot.get_team_name(999) == "Команда 999"


def test_build_team_room_alias(monkeypatch):
    bot = load_bot_module(monkeypatch)

    assert bot.build_team_room_alias(12) == "#team-12:insomniafest.ru"


def test_check_user_eligibility_cache_hit(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_record_id.clear()
    bot.grist_handle_to_person_name.clear()
    bot.grist_handle_to_team_memberships.clear()

    bot.grist_handle_to_record_id["alice"] = 123
    bot.grist_handle_to_person_name["alice"] = "Alice"
    bot.grist_handle_to_team_memberships["alice"] = {10: True, 11: False}

    eligible, check_ok, person_name, memberships = asyncio.run(bot.check_user_eligibility("@Alice"))

    assert eligible is True
    assert check_ok is True
    assert person_name == "Alice"
    assert memberships == {10: True, 11: False}


def test_check_user_eligibility_sync_failure_with_empty_cache(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_record_id.clear()
    bot.grist_handle_to_person_name.clear()
    bot.grist_handle_to_team_memberships.clear()

    async def fake_sync_grist_cache(force_full=False):
        return False

    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    eligible, check_ok, person_name, memberships = asyncio.run(bot.check_user_eligibility("@unknown"))

    assert eligible is False
    assert check_ok is False
    assert person_name is None
    assert memberships == {}


def test_sync_grist_cache_builds_team_memberships_and_teams(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_record_id.clear()
    bot.grist_handle_to_person_name.clear()
    bot.grist_handle_to_team_memberships.clear()
    bot.grist_team_id_to_name.clear()

    participations = [
        {
            "id": 1,
            "fields": {
                "id": 1,
                "Telegram2": "@alice",
                "person_name": "Alice",
                "team": 72,
                "role_code": "ORGANIZER",
            },
        },
        {
            "id": 2,
            "fields": {
                "id": 2,
                "Telegram2": "@alice",
                "person_name": "Alice",
                "team": 73,
                "role_code": "PARTICIPANT",
            },
        },
        {
            "id": 3,
            "fields": {
                "id": 3,
                "Telegram2": "@bob",
                "person_name": "Bob",
                "team": 72,
                "role_code": "PARTICIPANT",
            },
        },
    ]
    teams = [
        {"id": 72, "fields": {"team_name": "Точка сборки"}},
        {"id": 73, "fields": {"team_name": "Лес"}},
    ]

    async def fake_fetch_grist_records_via_records_api():
        return participations

    async def fake_fetch_grist_teams_via_records_api():
        return teams

    monkeypatch.setattr(bot, "fetch_grist_records_via_records_api", fake_fetch_grist_records_via_records_api)
    monkeypatch.setattr(bot, "fetch_grist_teams_via_records_api", fake_fetch_grist_teams_via_records_api)

    ok = asyncio.run(bot.sync_grist_cache(force_full=True))

    assert ok is True
    assert bot.grist_team_id_to_name == {72: "Точка сборки", 73: "Лес"}
    assert bot.grist_handle_to_team_memberships["alice"] == {72: True, 73: False}
    assert bot.grist_handle_to_team_memberships["bob"] == {72: False}


def test_require_admin_denies_non_admin(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    update = DummyUpdate(user_id=999, username="nope")

    allowed = asyncio.run(bot.require_admin(update))

    assert allowed is False
    assert update.message.sent
    assert "недоступна" in update.message.sent[0]["text"].lower()


def test_ops_sync_admin_success(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_sync_grist_cache(force_full=False):
        assert force_full is True
        bot.grist_handle_to_record_id.clear()
        bot.grist_team_id_to_name.clear()
        bot.grist_handle_to_record_id["alice"] = 1
        bot.grist_team_id_to_name[2] = "GR"
        bot.grist_max_record_id = 6178
        return True

    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext()

    asyncio.run(bot.ops_sync(update, context))

    assert update.message.sent
    text = update.message.sent[0]["text"]
    assert "Sync complete" in text
    assert "users=1" in text
    assert "teams=1" in text


def test_ops_check_reports_memberships(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)
    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name.update({2: "2026.GR(Организатор)"})

    async def fake_check_user_eligibility(handle):
        assert handle == "@test_member"
        return True, True, "Test Person", {2: True}

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@test_member"])

    asyncio.run(bot.ops_check(update, context))

    assert update.message.sent
    text = update.message.sent[0]["text"]
    assert "Eligible" in text
    assert "Test Person" in text
    assert "team=2" in text
    assert "organizer=true" in text


def test_ops_register_reports_full_flow_results(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)
    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name.update({1: "OneLab", 2: "GR"})

    async def fake_check_user_eligibility(handle):
        return True, True, "Test Person", {1: False, 2: True}

    async def fake_register_synapse_user(username, password):
        assert username == "test_member"
        assert isinstance(password, str)
        return True, None

    async def fake_set_synapse_display_name(username, display_name):
        assert username == "test_member"
        assert display_name == "Test Person"
        return True

    async def fake_join_user_to_rooms(username, rooms):
        assert username == "test_member"
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        assert username == "test_member"
        assert memberships == {1: False, 2: True}
        return False, ["GR"], ["GR"]

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@test_member"])

    asyncio.run(bot.ops_register(update, context))

    assert update.message.sent
    text = update.message.sent[0]["text"]
    assert "Admin full registration" in text
    assert "mxid=@test_member:insomniafest.ru" in text
    assert "created=true" in text
    assert "default_join_ok=true" in text
    assert "team_join_ok=false" in text
    assert "failed_team_rooms=GR" in text
    assert "failed_moderation_rooms=GR" in text


def test_normalize_room_alias_and_localpart(monkeypatch):
    bot = load_bot_module(monkeypatch)

    assert bot.normalize_room_alias("fake-1") == "#fake-1:insomniafest.ru"
    assert bot.normalize_room_alias("#fake-2") == "#fake-2:insomniafest.ru"
    assert bot.normalize_room_alias("#x:insomniafest.ru") == "#x:insomniafest.ru"

    assert bot.sanitize_fake_localpart("@fake-user") == "fake-user"
    assert bot.sanitize_fake_localpart("@fake-user:insomniafest.ru") == "fake-user"


def test_ops_fake_register_requires_fake_prefix(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["alice"])

    asyncio.run(bot.ops_fake_register(update, context))

    assert update.message.sent
    assert "must start with 'fake-'" in update.message.sent[0]["text"]


def test_ops_fake_register_success(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_register_synapse_user(username, password):
        assert username == "fake-1"
        assert isinstance(password, str)
        assert password
        return True, None

    async def fake_join_user_to_rooms(username, rooms):
        assert username == "fake-1"
        assert rooms == ["#fake-1:insomniafest.ru", "#fake-2:insomniafest.ru"]
        return True, []

    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["fake-1", "#fake-1", "#fake-2"])

    asyncio.run(bot.ops_fake_register(update, context))

    assert update.message.sent
    text = update.message.sent[0]["text"]
    assert "Fake registration smoke test" in text
    assert "mxid=@fake-1:insomniafest.ru" in text
    assert "created=true" in text
    assert "join_ok=true" in text


def test_sync_grist_cache_handles_real_grist_schema(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_record_id.clear()
    bot.grist_handle_to_person_name.clear()
    bot.grist_handle_to_team_memberships.clear()
    bot.grist_team_id_to_name.clear()

    teams = [
        {
            "id": 1,
            "fields": {
                "team_name": "1L Лаборатория",
            },
        },
        {
            "id": 2,
            "fields": {
                "team_name": "2026.GR(Организатор)",
            },
        },
    ]
    participations = [
        {
            "id": 6178,
            "fields": {
                "Telegram2": ["", "@test_member"],
                "person_name": "Test Person",
                "team": "2",
                "role_code": "ORGANIZER",
            },
        },
        {
            "id": 6179,
            "fields": {
                "Telegram2": "@test_member",
                "person_name": "Test Person",
                "team": 1,
                "role_code": "PARTICIPANT",
            },
        },
    ]

    async def fake_fetch_grist_records_via_records_api():
        return participations

    async def fake_fetch_grist_teams_via_records_api():
        return teams

    monkeypatch.setattr(bot, "fetch_grist_records_via_records_api", fake_fetch_grist_records_via_records_api)
    monkeypatch.setattr(bot, "fetch_grist_teams_via_records_api", fake_fetch_grist_teams_via_records_api)

    ok = asyncio.run(bot.sync_grist_cache(force_full=True))

    assert ok is True
    assert bot.grist_team_id_to_name == {1: "1L Лаборатория", 2: "2026.GR(Организатор)"}
    assert bot.grist_handle_to_record_id["test_member"] == 6179
    assert bot.grist_handle_to_person_name["test_member"] == "Test Person"
    assert bot.grist_handle_to_team_memberships["test_member"] == {2: True, 1: False}


def test_join_user_to_team_rooms_sets_moderator_only_for_organizers(monkeypatch):
    bot = load_bot_module(monkeypatch)

    ensured = []
    joined = []
    moderator = []

    async def fake_ensure_team_room(team_id, team_name):
        ensured.append((team_id, team_name))
        return f"!room{team_id}:insomniafest.ru"

    async def fake_join_user_to_rooms(username, rooms):
        joined.append((username, tuple(rooms)))
        return True, []

    async def fake_set_room_moderator(room_id, user_id):
        moderator.append((room_id, user_id))
        return True

    monkeypatch.setattr(bot, "ensure_team_room", fake_ensure_team_room)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "set_room_moderator", fake_set_room_moderator)

    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name.update({72: "Точка сборки", 73: "Лес"})

    ok, failed_team_rooms, failed_moderation_rooms = asyncio.run(
        bot.join_user_to_team_rooms("alice", {72: True, 73: False})
    )

    assert ok is True
    assert failed_team_rooms == []
    assert failed_moderation_rooms == []
    assert len(ensured) == 2
    assert len(joined) == 2
    assert len(moderator) == 1
    assert moderator[0][0] == "!room72:insomniafest.ru"


def test_join_user_to_team_rooms_collects_failed_rooms(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_ensure_team_room(team_id, team_name):
        if team_id == 72:
            return None
        return f"!room{team_id}:insomniafest.ru"

    async def fake_join_user_to_rooms(username, rooms):
        return True, []

    async def fake_set_room_moderator(room_id, user_id):
        return True

    monkeypatch.setattr(bot, "ensure_team_room", fake_ensure_team_room)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "set_room_moderator", fake_set_room_moderator)

    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name.update({72: "Точка сборки", 73: "Лес"})

    ok, failed_team_rooms, failed_moderation_rooms = asyncio.run(
        bot.join_user_to_team_rooms("alice", {72: True, 73: False})
    )

    assert ok is False
    assert failed_team_rooms == ["Точка сборки"]
    assert failed_moderation_rooms == []


def test_sync_grist_cache_throttles_without_fetch(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_record_id.clear()
    bot.grist_handle_to_record_id["cached"] = 1
    bot.grist_last_full_sync = bot.time.time()

    async def fail_fetch_records():
        raise AssertionError("records fetch should not be called")

    async def fail_fetch_teams():
        raise AssertionError("teams fetch should not be called")

    monkeypatch.setattr(bot, "fetch_grist_records_via_records_api", fail_fetch_records)
    monkeypatch.setattr(bot, "fetch_grist_teams_via_records_api", fail_fetch_teams)

    ok = asyncio.run(bot.sync_grist_cache(force_full=False))
    assert ok is True


def test_sync_grist_cache_records_fetch_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_record_id.clear()

    async def fail_fetch_records():
        raise RuntimeError("boom")

    monkeypatch.setattr(bot, "fetch_grist_records_via_records_api", fail_fetch_records)

    ok = asyncio.run(bot.sync_grist_cache(force_full=True))
    assert ok is False


def test_register_synapse_user_success(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_request_with_retries(client, method, url, **kwargs):
        if method == "GET":
            return FakeResponse(200, {"nonce": "abc"})

    async def fake_post(self, url, **kwargs):
        return FakeResponse(200, {})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(bot.httpx.AsyncClient, "post", fake_post)

    ok, code = asyncio.run(bot.register_synapse_user("alice", "pwd"))
    assert ok is True
    assert code is None


def test_register_synapse_user_nonce_missing(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(200, {})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.register_synapse_user("alice", "pwd"))
    assert ok is False
    assert code == "NONCE_MISSING"


def test_register_synapse_user_user_in_use(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_request_with_retries(client, method, url, **kwargs):
        if method == "GET":
            return FakeResponse(200, {"nonce": "abc"})
        raise AssertionError("POST should not use request_with_retries")

    async def fake_post(self, url, **kwargs):
        return FakeResponse(400, {"errcode": "M_USER_IN_USE"}, text="in use")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(bot.httpx.AsyncClient, "post", fake_post)

    ok, code = asyncio.run(bot.register_synapse_user("alice", "pwd"))
    assert ok is False
    assert code == "M_USER_IN_USE"


def test_register_synapse_user_retries_with_fresh_nonce(monkeypatch):
    bot = load_bot_module(monkeypatch)

    nonces = ["nonce-1", "nonce-2"]
    used_nonces = []

    async def fake_request_with_retries(client, method, url, **kwargs):
        assert method == "GET"
        return FakeResponse(200, {"nonce": nonces.pop(0)})

    async def fake_post(self, url, **kwargs):
        used_nonces.append(kwargs["json"]["nonce"])
        if len(used_nonces) == 1:
            return FakeResponse(400, {"errcode": "M_UNKNOWN"}, text="unrecognised nonce")
        return FakeResponse(200, {})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(bot.httpx.AsyncClient, "post", fake_post)

    ok, code = asyncio.run(bot.register_synapse_user("alice", "pwd"))

    assert ok is True
    assert code is None
    assert used_nonces == ["nonce-1", "nonce-2"]


def test_reactivate_synapse_user_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    responses = [
        FakeResponse(200, {"deactivated": True}),
        FakeResponse(200, {}),
    ]

    async def fake_request_with_retries(client, method, url, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.reactivate_synapse_user("alice", "pwd"))
    assert ok is True
    assert code is None


def test_reactivate_synapse_user_account_active(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(200, {"deactivated": False})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.reactivate_synapse_user("alice", "pwd"))
    assert ok is False
    assert code == "ACCOUNT_ACTIVE"


def test_reset_synapse_password_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(200, {})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.reset_synapse_password("alice", "pwd"))
    assert ok is True
    assert code is None


def test_reset_synapse_password_no_token(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    ok, code = asyncio.run(bot.reset_synapse_password("alice", "pwd"))
    assert ok is False
    assert code == "RESET_TOKEN_MISSING"


def test_reset_synapse_password_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(404, {}, text="not found")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.reset_synapse_password("alice", "pwd"))
    assert ok is False
    assert code == "RESET_FAILED"


def test_register_synapse_user_exception(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fail_request_with_retries(client, method, url, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(bot, "request_with_retries", fail_request_with_retries)

    ok, code = asyncio.run(bot.register_synapse_user("alice", "pwd"))
    assert ok is False
    assert code == "REGISTER_EXCEPTION"


def test_join_user_to_rooms_no_token(monkeypatch):
    # Missing token means auto-join is not configured; treated as success (no-op),
    # not a failure, to avoid false-positive owner warnings when Synapse handles it.
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    ok, failed = asyncio.run(bot.join_user_to_rooms("alice", ["#general:insomniafest.ru"]))
    assert ok is True
    assert failed == []


def test_join_user_to_rooms_partial_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    responses = [
        FakeResponse(200, {}),
        FakeResponse(500, {}, text="error"),
    ]

    async def fake_request_with_retries(client, method, url, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, failed = asyncio.run(bot.join_user_to_rooms("alice", ["room1", "room2"]))
    assert ok is False
    assert failed == ["room2"]


def test_resolve_room_alias_no_token(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    room_id = asyncio.run(bot.resolve_room_alias("#team-1:insomniafest.ru"))
    assert room_id is None


def test_resolve_room_alias_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(200, {"room_id": "!abc:insomniafest.ru"})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    room_id = asyncio.run(bot.resolve_room_alias("#team-1:insomniafest.ru"))
    assert room_id == "!abc:insomniafest.ru"


def test_create_team_room_retry_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    responses = [
        FakeResponse(409, {}, text="alias exists"),
    ]

    async def fake_request_with_retries(client, method, url, **kwargs):
        return responses.pop(0)

    async def fake_resolve_room_alias(alias):
        return "!new:insomniafest.ru"

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(bot, "resolve_room_alias", fake_resolve_room_alias)

    room_id = asyncio.run(bot.create_team_room(72, "Точка сборки"))
    assert room_id == "!new:insomniafest.ru"


def test_set_room_moderator_already_has_level(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    calls = []

    async def fake_request_with_retries(client, method, url, **kwargs):
        calls.append(method)
        return FakeResponse(200, {"users": {"@alice:insomniafest.ru": 100}})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok = asyncio.run(bot.set_room_moderator("!room:insomniafest.ru", "@alice:insomniafest.ru"))
    assert ok is True
    assert calls == ["GET"]


def test_start_command(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate()
    context = DummyContext()

    asyncio.run(bot.start(update, context))

    assert len(update.message.sent) == 1
    assert "/register" in update.message.sent[0]["text"]


def test_help_command(monkeypatch):
    bot = load_bot_module(monkeypatch)
    bot.ADMIN_TELEGRAM_IDS.clear()

    update = DummyUpdate()
    context = DummyContext()

    asyncio.run(bot.help_command(update, context))

    assert len(update.message.sent) == 1
    assert bot.HELP_URL in update.message.sent[0]["text"]
    assert "/reset_password" in update.message.sent[0]["text"]
    assert "Команды владельца" not in update.message.sent[0]["text"]
    assert update.message.sent[0]["parse_mode"] is None


def test_reset_password_rate_limited(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    now = 1_000_000.0
    monkeypatch.setattr(bot.time, "time", lambda: now)

    bot.user_registration_times.clear()
    bot.user_registration_times[42] = now - 10

    asyncio.run(bot.reset_password(update, context))

    assert len(update.message.sent) == 1
    assert "Подождите" in update.message.sent[0]["text"]


def test_reset_password_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="CaseMix_123")
    context = DummyContext()

    bot.user_registration_times.clear()
    captured = {}

    async def fake_check_user_eligibility(username):
        captured["eligibility"] = username
        return True, True, "Example User", {72: False}

    async def fake_reset_synapse_password(username, password):
        captured["reset"] = username
        return True, None

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "reset_synapse_password", fake_reset_synapse_password)

    asyncio.run(bot.reset_password(update, context))

    assert captured == {
        "eligibility": "casemix_123",
        "reset": "casemix_123",
    }
    assert len(update.message.sent) == 2
    assert "Пароль сброшен" in update.message.sent[1]["text"]
    assert update.message.sent[1]["parse_mode"] == bot.ParseMode.MARKDOWN


def test_help_command_admin_includes_owner_commands(monkeypatch):
    bot = load_bot_module(monkeypatch)
    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext()

    asyncio.run(bot.help_command(update, context))

    assert len(update.message.sent) == 1
    text = update.message.sent[0]["text"]
    assert "Команды админов" in text
    assert "/ops_sync" in text
    assert "/ops_check" in text
    assert "/ops_register" in text
    assert "/ops_fake_register" in text


def test_register_rate_limited(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    now = 1_000_000.0
    monkeypatch.setattr(bot.time, "time", lambda: now)

    bot.user_registration_times.clear()
    bot.user_registration_times[42] = now - 10

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 1
    assert "Подождите" in update.message.sent[0]["text"]


def test_register_eligibility_check_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()

    async def fake_check_user_eligibility(username):
        return False, False, None, {}

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    assert "Проверяю вашу благонадежность" in update.message.sent[0]["text"]
    assert "Не удалось проверить данные регистрации" in update.message.sent[1]["text"]


def test_register_not_eligible(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()

    async def fake_check_user_eligibility(username):
        return False, True, None, {}

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    assert "Ничего не вышло" in update.message.sent[1]["text"]


def test_register_user_in_use(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()

    async def fake_check_user_eligibility(username):
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        return False, "M_USER_IN_USE"

    async def fake_reactivate_synapse_user(username, password):
        return False, "ACCOUNT_ACTIVE"

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "reactivate_synapse_user", fake_reactivate_synapse_user)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    assert "уже существует" in update.message.sent[1]["text"]


def test_register_user_in_use_reactivated(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()

    async def fake_check_user_eligibility(username):
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        return False, "M_USER_IN_USE"

    async def fake_reactivate_synapse_user(username, password):
        return True, None

    async def fake_set_synapse_display_name(username, display_name):
        return True

    async def fake_join_user_to_rooms(username, room_aliases):
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        return True, [], []

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "reactivate_synapse_user", fake_reactivate_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    assert "Поздравляем" in update.message.sent[1]["text"]
    assert "аккаунт был восстановлен" in update.message.sent[1]["text"]


def test_register_success_happy_path(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()

    async def fake_check_user_eligibility(username):
        return True, True, "Alice", {72: True, 73: False}

    async def fake_register_synapse_user(username, password):
        return True, None

    async def fake_set_synapse_display_name(username, display_name):
        return True

    async def fake_join_user_to_rooms(username, room_aliases):
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        return True, [], []

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    assert "Поздравляем" in update.message.sent[1]["text"]
    assert update.message.sent[1]["parse_mode"] == bot.ParseMode.MARKDOWN


def test_register_normalizes_mixed_case_username(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="CaseMix_123")
    context = DummyContext()

    bot.user_registration_times.clear()
    captured = {}

    async def fake_check_user_eligibility(username):
        captured["eligibility"] = username
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        captured["register"] = username
        return True, None

    async def fake_set_synapse_display_name(username, display_name):
        captured["displayname"] = username
        return True

    async def fake_join_user_to_rooms(username, room_aliases):
        captured["join_rooms"] = username
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        captured["join_team_rooms"] = username
        return True, [], []

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    asyncio.run(bot.register(update, context))

    assert captured == {
        "eligibility": "casemix_123",
        "register": "casemix_123",
        "displayname": "casemix_123",
        "join_rooms": "casemix_123",
        "join_team_rooms": "casemix_123",
    }


def test_register_success_with_join_failures(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()
    notified = []

    async def fake_check_user_eligibility(username):
        return True, True, "Alice", {72: True}

    async def fake_register_synapse_user(username, password):
        return True, None

    async def fake_set_synapse_display_name(username, display_name):
        return True

    async def fake_join_user_to_rooms(username, room_aliases):
        return False, ["#general:insomniafest.ru"]

    async def fake_join_user_to_team_rooms(username, memberships):
        return False, ["Точка сборки"], ["Точка сборки"]

    async def fake_notify_owner(context_obj, message):
        notified.append(message)

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)
    monkeypatch.setattr(bot, "notify_owner", fake_notify_owner)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 5
    assert "не удалось автоматически добавить вас в комнаты" in update.message.sent[2]["text"]
    assert "не удалось автоматически добавить вас в командные комнаты" in update.message.sent[3]["text"]
    assert "не удалось выдать права модератора" in update.message.sent[4]["text"]
    assert len(notified) == 3


def test_register_notifies_owner_on_registration_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()
    notified = []

    async def fake_check_user_eligibility(username):
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        return False, "M_UNKNOWN"

    async def fake_notify_owner(context_obj, message):
        notified.append(message)

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "notify_owner", fake_notify_owner)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    assert "Не удалось создать учетную запись" in update.message.sent[1]["text"]
    assert len(notified) == 1
    assert "registration_error=M_UNKNOWN" in notified[0]


def test_register_exception_path(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()
    notified = []

    async def fail_check_user_eligibility(username):
        raise RuntimeError("test failure")

    async def fake_notify_owner(context_obj, message):
        notified.append(message)

    monkeypatch.setattr(bot, "check_user_eligibility", fail_check_user_eligibility)
    monkeypatch.setattr(bot, "notify_owner", fake_notify_owner)

    asyncio.run(bot.register(update, context))

    assert "Произошла ошибка при регистрации" in update.message.sent[-1]["text"]
    assert len(notified) == 1


def test_error_handler_sends_owner_and_user_message(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "Update", DummyUpdate)
    update = DummyUpdate(user_id=42, username="alice", chat_id=999)
    context = DummyContext(error=RuntimeError("boom"))

    notified = []

    async def fake_notify_owner(context_obj, message):
        notified.append(message)

    monkeypatch.setattr(bot, "notify_owner", fake_notify_owner)

    asyncio.run(bot.error_handler(update, context))

    assert len(notified) == 1
    assert len(context.bot.sent) == 1
    assert context.bot.sent[0]["chat_id"] == 999


def test_format_exception_chain_compacts_causes(monkeypatch):
    bot = load_bot_module(monkeypatch)

    root = OSError("network is unreachable")
    wrapped = RuntimeError("transport failed")
    wrapped.__cause__ = root

    result = bot.format_exception_chain(wrapped)

    assert "RuntimeError: transport failed" in result
    assert "OSError: network is unreachable" in result
    assert " <- " in result


def test_check_synapse_admin_token_success(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_get(self, url, headers=None, **kwargs):
        return FakeResponse(200, {"server_version": "1.0"})

    monkeypatch.setattr(bot.httpx.AsyncClient, "get", fake_get)

    ok, err = asyncio.run(bot.check_synapse_admin_token())
    assert ok is True
    assert err is None


def test_check_synapse_admin_token_rejected(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_get(self, url, headers=None, **kwargs):
        return FakeResponse(403, {})

    monkeypatch.setattr(bot.httpx.AsyncClient, "get", fake_get)

    ok, err = asyncio.run(bot.check_synapse_admin_token())
    assert ok is False
    assert "403" in err


def test_check_synapse_admin_token_unreachable(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_get(self, url, headers=None, **kwargs):
        raise bot.httpx.ConnectError("connection refused")

    monkeypatch.setattr(bot.httpx.AsyncClient, "get", fake_get)

    ok, err = asyncio.run(bot.check_synapse_admin_token())
    assert ok is False
    assert "Synapse" in err



    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "Update", DummyUpdate)
    update = DummyUpdate(user_id=42, username="alice", chat_id=999)

    root = OSError("network is unreachable")
    net_err = bot.NetworkError("httpx.ConnectError: All connection attempts failed")
    net_err.__cause__ = root
    context = DummyContext(error=net_err)

    warnings = []
    notified = []

    async def fake_notify_owner(context_obj, message):
        notified.append(message)

    def fake_warning(message, *args, **kwargs):
        warnings.append(message % args if args else message)

    monkeypatch.setattr(bot, "notify_owner", fake_notify_owner)
    monkeypatch.setattr(bot.logger, "warning", fake_warning)

    asyncio.run(bot.error_handler(update, context))

    assert len(notified) == 0
    assert len(context.bot.sent) == 0
    assert len(warnings) == 1
    assert "NetworkError: httpx.ConnectError: All connection attempts failed" in warnings[0]
    assert "OSError: network is unreachable" in warnings[0]
