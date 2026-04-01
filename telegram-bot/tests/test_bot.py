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

    async def reply_text(self, text, parse_mode=None, reply_markup=None):
        self.sent.append({"text": text, "parse_mode": parse_mode, "reply_markup": reply_markup})


class DummyUser:
    def __init__(self, user_id=1, username="alice"):
        self.id = user_id
        self.username = username


class DummyChat:
    def __init__(self, chat_id=1):
        self.id = chat_id


class DummyUpdate:
    def __init__(self, user_id=1, username="alice", chat_id=1, is_edited=False):
        self.effective_user = DummyUser(user_id=user_id, username=username)
        self.message = None if is_edited else DummyMessage()
        self.edited_message = DummyMessage() if is_edited else None
        self.effective_message = self.edited_message if is_edited else self.message
        self.effective_chat = DummyChat(chat_id=chat_id)


class DummyCallbackQuery:
    def __init__(self, message=None):
        self.message = message if message is not None else DummyMessage()
        self.last_reply_markup = "__not_set__"
        self.answers = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append({"text": text, "show_alert": show_alert})

    async def edit_message_reply_markup(self, reply_markup=None):
        self.last_reply_markup = reply_markup


class DummyCallbackUpdate:
    def __init__(self, user_id=1, username="alice", chat_id=1, message=None):
        self.effective_user = DummyUser(user_id=user_id, username=username)
        self.effective_chat = DummyChat(chat_id=chat_id)
        self.callback_query = DummyCallbackQuery(message=message)
        self.message = None
        self.edited_message = None
        self.effective_message = None


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


def test_registration_localpart_from_handle(monkeypatch):
    bot = load_bot_module(monkeypatch)

    assert bot.registration_localpart_from_handle("@$Alice") == "alice"
    assert bot.registration_localpart_from_handle("$$$") == "$$$"
    assert bot.registration_localpart_from_handle("normal_user") == "normal_user"


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


def test_sync_grist_people_matrix_cache_builds_registration_mappings(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_fetch_grist_people_via_records_api():
        return [
            {
                "id": 21,
                "fields": {
                    "id": 21,
                    "name": "Alice",
                    "Telegram": "@Alice",
                    "telegram_norm_TF": "alice",
                    "matrix_id": "@alice:insomniafest.ru",
                    "isInBL": "Нет",
                },
            },
            {
                "id": 22,
                "fields": {
                    "id": 22,
                    "name": "Bob",
                    "Telegram": "@bob",
                    "telegram_norm_TF": "bob",
                    "matrix_id": "@bob:insomniafest.ru",
                    "isInBL": "Да",
                },
            },
        ]

    monkeypatch.setattr(bot, "fetch_grist_people_via_records_api", fake_fetch_grist_people_via_records_api)

    ok = asyncio.run(bot.sync_grist_people_matrix_cache(force_full=True))

    assert ok is True
    assert bot.grist_people_handle_to_person_row_id["alice"] == 21
    assert bot.grist_people_handle_to_person_row_id["bob"] == 22
    assert bot.grist_matrix_id_to_person_row_id["@alice:insomniafest.ru"] == 21
    assert bot.grist_people_person_row_to_person_name[21] == "Alice"
    assert bot.grist_people_person_row_to_is_not_blacklisted[21] is True
    assert bot.grist_people_person_row_to_is_not_blacklisted[22] is False


def test_check_registration_eligibility_fake_handle_uses_people_row(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_people_handle_to_person_row_id.clear()
    bot.grist_people_person_row_to_person_name.clear()
    bot.grist_people_person_row_to_is_not_blacklisted.clear()
    bot.grist_handle_to_person_row_id.clear()
    bot.grist_person_row_to_team_memberships.clear()

    bot.grist_people_person_row_to_person_name[21] = "Alice"
    bot.grist_people_person_row_to_is_not_blacklisted[21] = True
    bot.grist_handle_to_person_row_id["$alice"] = 21
    bot.grist_person_row_to_team_memberships[21] = {72: True}

    async def fake_sync_grist_people_matrix_cache(force_full=False):
        return True

    async def fake_sync_grist_cache(force_full=False):
        return True

    monkeypatch.setattr(bot, "sync_grist_people_matrix_cache", fake_sync_grist_people_matrix_cache)
    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    eligible, check_ok, person_name, memberships = asyncio.run(
        bot.check_registration_eligibility("@$alice")
    )

    assert eligible is True
    assert check_ok is True
    assert person_name == "Alice"
    assert memberships == {72: True}


def test_check_registration_eligibility_blacklisted_person(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_people_handle_to_person_row_id.clear()
    bot.grist_people_person_row_to_person_name.clear()
    bot.grist_people_person_row_to_is_not_blacklisted.clear()

    bot.grist_people_handle_to_person_row_id["alice"] = 21
    bot.grist_people_person_row_to_person_name[21] = "Alice"
    bot.grist_people_person_row_to_is_not_blacklisted[21] = False

    async def fake_sync_grist_people_matrix_cache(force_full=False):
        return True

    async def fake_sync_grist_cache(force_full=False):
        return True

    monkeypatch.setattr(bot, "sync_grist_people_matrix_cache", fake_sync_grist_people_matrix_cache)
    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    eligible, check_ok, person_name, memberships = asyncio.run(
        bot.check_registration_eligibility("@alice")
    )

    assert eligible is False
    assert check_ok is True
    assert person_name == "Alice"
    assert memberships == {}


def test_check_people_status_by_telegram_handle(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_people_handle_to_person_row_id.clear()
    bot.grist_people_person_row_to_person_name.clear()
    bot.grist_people_person_row_to_is_not_blacklisted.clear()

    bot.grist_people_handle_to_person_row_id["alice"] = 21
    bot.grist_people_person_row_to_person_name[21] = "Alice"
    bot.grist_people_person_row_to_is_not_blacklisted[21] = True

    async def fake_sync_grist_people_matrix_cache(force_full=False):
        return True

    monkeypatch.setattr(bot, "sync_grist_people_matrix_cache", fake_sync_grist_people_matrix_cache)

    found, check_ok, person_name, is_not_blacklisted = asyncio.run(
        bot.check_people_status("@alice")
    )

    assert found is True
    assert check_ok is True
    assert person_name == "Alice"
    assert is_not_blacklisted is True


def test_check_people_status_uses_participation_person_row_fallback(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_people_handle_to_person_row_id.clear()
    bot.grist_people_person_row_to_person_name.clear()
    bot.grist_people_person_row_to_is_not_blacklisted.clear()
    bot.grist_handle_to_person_row_id.clear()
    bot.grist_person_row_to_person_name.clear()

    bot.grist_handle_to_person_row_id["$alice"] = 21
    bot.grist_people_person_row_to_person_name[21] = "Alice"
    bot.grist_people_person_row_to_is_not_blacklisted[21] = False

    async def fake_sync_grist_people_matrix_cache(force_full=False):
        return True

    async def fake_sync_grist_cache(force_full=False):
        return True

    monkeypatch.setattr(bot, "sync_grist_people_matrix_cache", fake_sync_grist_people_matrix_cache)
    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    found, check_ok, person_name, is_not_blacklisted = asyncio.run(
        bot.check_people_status("@$alice")
    )

    assert found is True
    assert check_ok is True
    assert person_name == "Alice"
    assert is_not_blacklisted is False


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
                "isHR_Now": True,
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
    assert bot.grist_handle_to_is_hr_now["alice"] is True


def test_sync_grist_cache_collects_person_row_id(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_record_id.clear()
    bot.grist_handle_to_person_row_id.clear()

    participations = [
        {
            "id": 1,
            "fields": {
                "id": 1,
                "Telegram2": "@alice",
                "person_row_id": 21,
                "status_code": "PLANNED",
            },
        }
    ]

    async def fake_fetch_grist_records_via_records_api():
        return participations

    async def fake_fetch_grist_teams_via_records_api():
        return []

    monkeypatch.setattr(bot, "fetch_grist_records_via_records_api", fake_fetch_grist_records_via_records_api)
    monkeypatch.setattr(bot, "fetch_grist_teams_via_records_api", fake_fetch_grist_teams_via_records_api)

    ok = asyncio.run(bot.sync_grist_cache(force_full=True))
    assert ok is True
    assert bot.grist_handle_to_person_row_id["alice"] == 21


def test_update_grist_people_matrix_id_success(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_people_handle_to_person_row_id.clear()
    bot.grist_people_handle_to_person_row_id["alice"] = 21
    captured = {}

    async def fake_request_with_retries(client, method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = kwargs.get("json")
        return FakeResponse(200, {})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.update_grist_people_matrix_id("alice", "@alice:insomniafest.ru"))

    assert ok is True
    assert code is None
    assert captured["method"] == "PATCH"
    assert "/tables/People/records" in captured["url"]
    assert captured["json"]["records"][0]["id"] == 21
    assert captured["json"]["records"][0]["fields"]["matrix_id"] == "@alice:insomniafest.ru"


def test_update_grist_people_matrix_id_missing_person_row(monkeypatch):
    bot = load_bot_module(monkeypatch)
    bot.grist_people_handle_to_person_row_id.clear()
    bot.grist_handle_to_person_row_id.clear()

    ok, code = asyncio.run(bot.update_grist_people_matrix_id("alice", "@alice:insomniafest.ru"))
    assert ok is False
    assert code == "PERSON_ROW_ID_MISSING"


def test_require_admin_denies_non_admin(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    update = DummyUpdate(user_id=999, username="nope")

    allowed = asyncio.run(bot.require_admin(update))

    assert allowed is False
    assert update.message.sent
    assert "недоступна" in update.message.sent[0]["text"].lower()


def test_require_admin_allows_hr_from_grist_cache(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.grist_handle_to_is_hr_now.clear()
    bot.grist_handle_to_is_hr_now["alice"] = True

    update = DummyUpdate(user_id=999, username="alice")
    allowed = asyncio.run(bot.require_admin(update))

    assert allowed is True
    assert update.message.sent == []


def test_require_admin_allows_organizer_from_grist_cache(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.grist_handle_to_is_hr_now.clear()
    bot.grist_handle_to_team_memberships.clear()
    bot.grist_handle_to_team_memberships["alice"] = {72: True, 73: False}

    update = DummyUpdate(user_id=999, username="alice")
    allowed = asyncio.run(bot.require_admin(update))

    assert allowed is True
    assert update.message.sent == []


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
    assert text == "✅ Кэш Гриста обновлен\n✅ Людей в Участиях 2026: 1"


def test_ops_check_reports_memberships(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)
    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name.update({2: "2026.GR(Организатор)"})

    async def fake_get_people_status_details(handle):
        assert handle == "@test_member"
        return True, True, 21, "Test Person", True

    async def fake_check_user_eligibility(handle):
        assert handle == "@test_member"
        return True, True, "Test Person", {2: True}

    async def fake_get_synapse_registration_status(username):
        assert username == "test_member"
        return "registered"

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@test_member"])

    asyncio.run(bot.ops_check(update, context))

    assert update.message.sent
    text = update.message.sent[0]["text"]
    assert "Отчет по участнику" in text
    assert "👤 Telegram: @test_member" in text
    assert "Имя: Test Person" in text
    assert "ID в Человеках: 21" in text
    assert "Найден в Человеках: найден" in text
    assert "Черный список: нет" in text
    assert "Участия 2026: найден" in text
    assert "Регистрация в Matrix: уже зарегистрирован (@test_member:insomniafest.ru)" in text
    assert "Комната организаторов: да" in text
    assert "👥 Команды:" in text
    assert "• 2026.GR(Организатор) — организатор" in text


def test_ops_register_reports_full_flow_results(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)
    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name.update({1: "OneLab", 2: "GR"})

    async def fake_get_people_status_details(handle):
        assert handle == "@test_member"
        return True, True, 21, "Test Person", True

    async def fake_check_user_eligibility(handle):
        assert handle == "@test_member"
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

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
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
    assert update.message.sent[0]["parse_mode"] == bot.ParseMode.HTML
    assert "Полная регистрация через HR" in text
    assert "Telegram username: <code>test_member</code>" in text
    assert "Matrix MXID: <code>@test_member:insomniafest.ru</code>" in text
    assert "People: найден" in text
    assert "Person Row ID: 21" in text
    assert "Черный список: нет" in text
    assert "Участия 2026: найден" in text
    assert "Аккаунт создан: да" in text
    assert "Добавление в базовые комнаты: да" in text
    assert "Добавление в командные комнаты: нет" in text
    assert "Сообщение для пользователя" in text
    assert "<pre>Привет! Твой аккаунт в Бессонном Чате создан:" in text
    assert "Логин: test_member" in text
    assert "Временный пароль:" in text
    assert "Вход: https://chat.insomniafest.ru" in text
    assert "Если потеряешь логин или пароль, их можно получить через бота или у HR." in text
    assert "Участие в таблице не обязательно для входа в чат, но нужно для автоматического добавления в командный чат." in text
    assert "Инструкция: https://chat.insomniafest.ru/help" in text
    assert "Не добавлен в командные комнаты: GR" in text
    assert "Не выданы права администратора: GR" in text


def test_ops_join_teams_reports_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    captured = {}

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_check_user_eligibility(handle):
        assert handle == "@test_member"
        return True, True, "Test Person", {2: True, 5: False}

    async def fake_get_synapse_registration_status(username):
        assert username == "test_member"
        return "registered"

    async def fake_sync_user_to_team_rooms_detailed(username, memberships):
        assert username == "test_member"
        assert memberships == {2: True, 5: False}
        return [
            {
                "team_id": 2,
                "team_name": "2026.GR(Организатор)",
                "is_organizer": True,
                "room_id": "!team2:insomniafest.ru",
                "status": "already_joined",
            },
            {
                "team_id": 5,
                "team_name": "Медиа",
                "is_organizer": False,
                "room_id": "!team5:insomniafest.ru",
                "status": "joined",
            },
        ], []

    async def fake_join_user_to_rooms(username, rooms):
        captured["orgs_join_username"] = username
        captured["orgs_join_rooms"] = list(rooms)
        return True, []

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)
    monkeypatch.setattr(bot, "sync_user_to_team_rooms_detailed", fake_sync_user_to_team_rooms_detailed)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@test_member"])

    asyncio.run(bot.ops_join_teams(update, context))

    assert update.message.sent
    text = update.message.sent[0]["text"]
    assert "Проверил статус участия в командах" in text
    assert "Пользователь: @test_member" in text
    assert "Имя: Test Person" in text
    assert "Уже был в этих командных комнатах" in text
    assert "Был добавлен в эти комнаты" in text
    assert "2026.GR(Организатор)" in text
    assert update.message.sent[0]["parse_mode"] == bot.ParseMode.HTML
    assert captured["orgs_join_username"] == "test_member"
    assert captured["orgs_join_rooms"] == [bot.ORGS_ROOM]


def test_ops_join_teams_usage(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=[])

    asyncio.run(bot.ops_join_teams(update, context))

    assert update.message.sent
    assert "Использование: /hr_join_teams <телеграм_ник>" in update.message.sent[0]["text"]


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
    joined_spaces = []
    joined_rooms = []
    moderator = []

    async def fake_ensure_team_room(team_id, team_name):
        ensured.append((team_id, team_name))
        return f"!room{team_id}:insomniafest.ru"

    async def fake_get_room_parent_spaces(room_id):
        if room_id == "!room72:insomniafest.ru":
            return ["!space72:insomniafest.ru"]
        return []

    async def fake_join_user_to_rooms(username, rooms):
        joined_spaces.append((username, tuple(rooms)))
        return True, []

    async def fake_join_user_to_room(username, room_alias_or_id):
        joined_rooms.append((username, room_alias_or_id))
        return "joined"

    async def fake_set_room_moderator(room_id, user_id):
        moderator.append((room_id, user_id))
        return True

    monkeypatch.setattr(bot, "ensure_team_room", fake_ensure_team_room)
    monkeypatch.setattr(bot, "get_room_parent_spaces", fake_get_room_parent_spaces)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_room", fake_join_user_to_room)
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
    assert joined_spaces == [("alice", ("!space72:insomniafest.ru",))]
    assert joined_rooms == [
        ("alice", "!room72:insomniafest.ru"),
        ("alice", "!room73:insomniafest.ru"),
    ]
    assert len(moderator) == 1
    assert moderator[0][0] == "!room72:insomniafest.ru"


def test_join_user_to_team_rooms_collects_failed_rooms(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_ensure_team_room(team_id, team_name):
        if team_id == 72:
            return None
        return f"!room{team_id}:insomniafest.ru"

    async def fake_get_room_parent_spaces(room_id):
        return []

    async def fake_join_user_to_rooms(username, rooms):
        return True, []

    async def fake_join_user_to_room(username, room_alias_or_id):
        return "joined"

    async def fake_set_room_moderator(room_id, user_id):
        return True

    monkeypatch.setattr(bot, "ensure_team_room", fake_ensure_team_room)
    monkeypatch.setattr(bot, "get_room_parent_spaces", fake_get_room_parent_spaces)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_room", fake_join_user_to_room)
    monkeypatch.setattr(bot, "set_room_moderator", fake_set_room_moderator)

    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name.update({72: "Точка сборки", 73: "Лес"})

    ok, failed_team_rooms, failed_moderation_rooms = asyncio.run(
        bot.join_user_to_team_rooms("alice", {72: True, 73: False})
    )

    assert ok is False
    assert failed_team_rooms == ["Точка сборки"]
    assert failed_moderation_rooms == []


def test_get_room_parent_spaces_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(
            200,
            [
                {"type": "m.space.parent", "state_key": "!space1:insomniafest.ru"},
                {"type": "m.space.parent", "state_key": "!space2:insomniafest.ru"},
                {"type": "m.room.name", "state_key": ""},
                {"type": "m.space.parent", "state_key": "!space1:insomniafest.ru"},
            ],
        )

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    spaces = asyncio.run(bot.get_room_parent_spaces("!room:insomniafest.ru"))

    assert spaces == ["!space1:insomniafest.ru", "!space2:insomniafest.ru"]


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


def test_get_synapse_registration_status_registered(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        assert method == "GET"
        assert "/_synapse/admin/v2/users/" in url
        return FakeResponse(200, {"name": "@alice:insomniafest.ru", "deactivated": False})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    status = asyncio.run(bot.get_synapse_registration_status("alice"))

    assert status == "registered"


def test_get_synapse_registration_status_not_registered(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(404, {}, text="not found")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    status = asyncio.run(bot.get_synapse_registration_status("alice"))

    assert status == "not_registered"


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


def test_join_user_to_rooms_already_joined_is_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(
            403,
            {"errcode": "M_FORBIDDEN", "error": "@alice:insomniafest.ru is already in the room."},
            text='{"errcode":"M_FORBIDDEN","error":"@alice:insomniafest.ru is already in the room."}',
        )

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, failed = asyncio.run(bot.join_user_to_rooms("alice", ["!room:insomniafest.ru"]))
    assert ok is True
    assert failed == []


def test_join_user_to_room_reports_already_joined(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(
            403,
            {"errcode": "M_FORBIDDEN", "error": "@alice:insomniafest.ru is already in the room."},
            text='{"errcode":"M_FORBIDDEN","error":"@alice:insomniafest.ru is already in the room."}',
        )

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    status = asyncio.run(bot.join_user_to_room("alice", "!room:insomniafest.ru"))
    assert status == "already_joined"


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


def test_my_teams_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    captured = {}

    async def fake_check_user_eligibility(handle):
        assert handle == "alice"
        return True, True, "Alice", {2: True, 5: False}

    async def fake_get_synapse_registration_status(username):
        assert username == "alice"
        return "registered"

    async def fake_sync_user_to_team_rooms_detailed(username, memberships):
        assert username == "alice"
        assert memberships == {2: True, 5: False}
        return [
            {
                "team_id": 2,
                "team_name": "2026.GR(Организатор)",
                "is_organizer": True,
                "room_id": "!team2:insomniafest.ru",
                "status": "already_joined",
            },
            {
                "team_id": 5,
                "team_name": "Медиа",
                "is_organizer": False,
                "room_id": "!team5:insomniafest.ru",
                "status": "joined",
            },
        ], []

    async def fake_join_user_to_rooms(username, rooms):
        captured["orgs_join_username"] = username
        captured["orgs_join_rooms"] = list(rooms)
        return True, []

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)
    monkeypatch.setattr(bot, "sync_user_to_team_rooms_detailed", fake_sync_user_to_team_rooms_detailed)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)

    update = DummyUpdate(user_id=10, username="alice")
    context = DummyContext()

    asyncio.run(bot.join_teams(update, context))

    assert len(update.message.sent) == 2
    assert "Проверяю ваши команды" in update.message.sent[0]["text"]
    assert "Проверил статус вашего участия в командах" in update.message.sent[1]["text"]
    assert "2026.GR(Организатор)" in update.message.sent[1]["text"]
    assert "Вы уже были в этих командных комнатах" in update.message.sent[1]["text"]
    assert "Вы были добавлены в эти комнаты" in update.message.sent[1]["text"]
    assert update.message.sent[1]["parse_mode"] == bot.ParseMode.HTML
    assert captured["orgs_join_username"] == "alice"
    assert captured["orgs_join_rooms"] == [bot.ORGS_ROOM]


def test_my_teams_requires_registration(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_check_user_eligibility(handle):
        return True, True, "Alice", {2: True}

    async def fake_get_synapse_registration_status(username):
        return "not_registered"

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)

    update = DummyUpdate(user_id=10, username="alice")
    context = DummyContext()

    asyncio.run(bot.join_teams(update, context))

    assert len(update.message.sent) == 2
    assert "сначала используйте /register" in update.message.sent[1]["text"].lower()


def test_help_command_mentions_my_teams(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_is_hr_command_user(update):
        return False

    monkeypatch.setattr(bot, "is_hr_command_user", fake_is_hr_command_user)

    update = DummyUpdate(user_id=10, username="alice")
    context = DummyContext()

    asyncio.run(bot.help_command(update, context))

    assert update.message.sent
    assert "/join_teams" in update.message.sent[0]["text"]


def test_sync_user_to_team_rooms_detailed_reports_already_joined(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_ensure_team_room(team_id, team_name):
        return "!room72:insomniafest.ru"

    async def fake_get_room_parent_spaces(room_id):
        return []

    async def fake_join_user_to_room(username, room_alias_or_id):
        assert username == "alice"
        assert room_alias_or_id == "!room72:insomniafest.ru"
        return "already_joined"

    async def fake_set_room_moderator(room_id, user_id):
        return True

    monkeypatch.setattr(bot, "ensure_team_room", fake_ensure_team_room)
    monkeypatch.setattr(bot, "get_room_parent_spaces", fake_get_room_parent_spaces)
    monkeypatch.setattr(bot, "join_user_to_room", fake_join_user_to_room)
    monkeypatch.setattr(bot, "set_room_moderator", fake_set_room_moderator)

    results, failed_moderation_rooms = asyncio.run(
        bot.sync_user_to_team_rooms_detailed("alice", {72: True})
    )

    assert failed_moderation_rooms == []
    assert len(results) == 1
    assert results[0]["status"] == "already_joined"
    assert results[0]["room_id"] == "!room72:insomniafest.ru"


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


def test_help_command_hr_mentions_hr_join_teams(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_is_hr_command_user(update):
        return True

    monkeypatch.setattr(bot, "is_hr_command_user", fake_is_hr_command_user)

    update = DummyUpdate(user_id=10, username="hr_user")
    context = DummyContext()

    asyncio.run(bot.help_command(update, context))

    assert update.message.sent
    assert "/hr_join_teams" in update.message.sent[0]["text"]


def test_reset_password_rate_limited(monkeypatch):
    bot = load_bot_module(monkeypatch)
    base_update = DummyUpdate(user_id=42, username="alice")
    update = DummyCallbackUpdate(user_id=42, username="alice", message=base_update.message)
    context = DummyContext()

    now = 1_000_000.0
    monkeypatch.setattr(bot.time, "time", lambda: now)

    bot.user_registration_times.clear()
    bot.password_reset_confirmations.clear()
    bot.password_reset_confirmations[42] = now
    bot.user_registration_times[42] = now - 10

    asyncio.run(bot.reset_password_confirm_callback(update, context))

    assert len(base_update.message.sent) == 1
    assert "Подождите" in base_update.message.sent[0]["text"]


def test_reset_password_requires_confirmation(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.password_reset_confirmations.clear()

    asyncio.run(bot.reset_password(update, context))

    assert len(update.message.sent) == 1
    assert "выходу из аккаунта на всех устройствах" in update.message.sent[0]["text"]
    assert "нажмите кнопку ниже" in update.message.sent[0]["text"]
    assert update.message.sent[0]["reply_markup"] is not None
    assert 42 in bot.password_reset_confirmations


def test_reset_password_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    base_update = DummyUpdate(user_id=42, username="CaseMix_123")
    update = DummyCallbackUpdate(user_id=42, username="CaseMix_123", message=base_update.message)
    context = DummyContext()

    bot.user_registration_times.clear()
    bot.password_reset_confirmations.clear()
    bot.password_reset_confirmations[42] = 1_000_000.0
    monkeypatch.setattr(bot.time, "time", lambda: 1_000_000.0)
    captured = {}

    async def fake_check_registration_eligibility(username):
        captured["eligibility"] = username
        return True, True, "Example User", {72: False}

    async def fake_reset_synapse_password(username, password):
        captured["reset"] = username
        captured["password"] = password
        return True, None

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
    monkeypatch.setattr(bot, "reset_synapse_password", fake_reset_synapse_password)

    asyncio.run(bot.reset_password_confirm_callback(update, context))

    assert captured == {
        "eligibility": "casemix_123",
        "reset": "casemix_123",
        "password": captured["password"],
    }
    assert len(base_update.message.sent) == 2
    text = base_update.message.sent[1]["text"]
    assert "Пароль сброшен" in text
    assert "<b>Имя пользователя:</b>" in text
    assert "<code>casemix_123</code>" in text
    assert "\\_" not in text
    assert f"<code>{captured['password']}</code>" in text
    assert "<a href=\"https://chat.insomniafest.ru\">https://chat.insomniafest.ru</a>" in text
    assert "<a href=\"https://chat.insomniafest.ru/help\">https://chat.insomniafest.ru/help</a>" in text
    assert base_update.message.sent[1]["parse_mode"] == bot.ParseMode.HTML


def test_help_command_admin_includes_owner_commands(monkeypatch):
    bot = load_bot_module(monkeypatch)
    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext()

    asyncio.run(bot.help_command(update, context))

    assert len(update.message.sent) == 1
    text = update.message.sent[0]["text"]
    assert "Команды HR" in text
    assert "/hr_sync" in text
    assert "/hr_check" in text
    assert "/hr_register" in text


def test_help_command_hr_user_includes_hr_commands(monkeypatch):
    bot = load_bot_module(monkeypatch)
    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.grist_handle_to_is_hr_now.clear()
    bot.grist_handle_to_is_hr_now["alice"] = True

    update = DummyUpdate(user_id=999, username="alice")
    context = DummyContext()

    asyncio.run(bot.help_command(update, context))

    assert len(update.message.sent) == 1
    text = update.message.sent[0]["text"]
    assert "Команды HR" in text
    assert "/hr_sync" in text
    assert "/hr_check" in text
    assert "/hr_register" in text


def test_help_command_organizer_includes_hr_commands(monkeypatch):
    bot = load_bot_module(monkeypatch)
    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.grist_handle_to_is_hr_now.clear()
    bot.grist_handle_to_team_memberships.clear()
    bot.grist_handle_to_team_memberships["alice"] = {72: True}

    update = DummyUpdate(user_id=999, username="alice")
    context = DummyContext()

    asyncio.run(bot.help_command(update, context))

    assert len(update.message.sent) == 1
    text = update.message.sent[0]["text"]
    assert "Команды HR" in text
    assert "/hr_sync" in text
    assert "/hr_check" in text
    assert "/hr_register" in text


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

    async def fake_check_registration_eligibility(username):
        return False, False, None, {}

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    assert "Проверяю вашу благонадежность" in update.message.sent[0]["text"]
    assert "Не удалось проверить данные регистрации" in update.message.sent[1]["text"]


def test_register_not_eligible(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()

    async def fake_check_registration_eligibility(username):
        return False, True, None, {}

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    assert "Ничего не вышло" in update.message.sent[1]["text"]


def test_register_user_in_use(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()

    async def fake_check_registration_eligibility(username):
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        return False, "M_USER_IN_USE"

    async def fake_reactivate_synapse_user(username, password):
        return False, "ACCOUNT_ACTIVE"

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
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

    async def fake_check_registration_eligibility(username):
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

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
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
    captured = {}

    async def fake_check_registration_eligibility(username):
        return True, True, "Alice", {72: True, 73: False}

    async def fake_register_synapse_user(username, password):
        captured["password"] = password
        return True, None

    async def fake_set_synapse_display_name(username, display_name):
        return True

    async def fake_join_user_to_rooms(username, room_aliases):
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        return True, [], []

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 2
    text = update.message.sent[1]["text"]
    assert "Поздравляем" in text
    assert "<b>Имя пользователя:</b>" in text
    assert "<code>alice</code>" in text
    assert "<b>Временный пароль:</b>" in text
    assert f"<code>{captured['password']}</code>" in text
    assert "<a href=\"https://chat.insomniafest.ru\">браузерную версию</a>" in text
    assert "<a href=\"https://chat.insomniafest.ru/help\">https://chat.insomniafest.ru/help</a>" in text
    assert update.message.sent[1]["parse_mode"] == bot.ParseMode.HTML


def test_register_normalizes_mixed_case_username(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="CaseMix_123")
    context = DummyContext()

    bot.user_registration_times.clear()
    captured = {}

    async def fake_check_registration_eligibility(username):
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

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
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

    async def fake_check_registration_eligibility(username):
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

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)
    monkeypatch.setattr(bot, "notify_owner", fake_notify_owner)

    asyncio.run(bot.register(update, context))

    assert len(update.message.sent) == 5
    assert "не удалось автоматически добавить вас в комнаты" in update.message.sent[2]["text"]
    assert "не удалось автоматически добавить вас в командные комнаты" in update.message.sent[3]["text"]
    assert "не удалось выдать права администратора" in update.message.sent[4]["text"]
    assert len(notified) == 3


def test_register_notifies_owner_on_registration_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="alice")
    context = DummyContext()

    bot.user_registration_times.clear()
    notified = []

    async def fake_check_registration_eligibility(username):
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        return False, "M_UNKNOWN"

    async def fake_notify_owner(context_obj, message):
        notified.append(message)

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
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

    async def fail_check_registration_eligibility(username):
        raise RuntimeError("test failure")

    async def fake_notify_owner(context_obj, message):
        notified.append(message)

    monkeypatch.setattr(bot, "check_registration_eligibility", fail_check_registration_eligibility)
    monkeypatch.setattr(bot, "notify_owner", fake_notify_owner)

    asyncio.run(bot.register(update, context))

    assert "Произошла ошибка при регистрации" in update.message.sent[-1]["text"]
    assert len(notified) == 1


def test_register_fake_tg_uses_stripped_localpart(monkeypatch):
    bot = load_bot_module(monkeypatch)
    update = DummyUpdate(user_id=42, username="$Alice")
    context = DummyContext()

    bot.user_registration_times.clear()
    captured = {}

    async def fake_check_registration_eligibility(handle):
        captured["eligibility_handle"] = handle
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        captured["register_username"] = username
        return True, None

    async def fake_set_synapse_display_name(username, display_name):
        return True

    async def fake_update_grist_people_matrix_id(handle, matrix_id):
        captured["update_people_handle"] = handle
        captured["matrix_id"] = matrix_id
        return True, None

    async def fake_join_user_to_rooms(username, room_aliases):
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        return True, [], []

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "update_grist_people_matrix_id", fake_update_grist_people_matrix_id)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    asyncio.run(bot.register(update, context))

    assert captured["eligibility_handle"] == "$alice"
    assert captured["register_username"] == "alice"
    assert captured["update_people_handle"] == "$alice"
    assert captured["matrix_id"] == "@alice:insomniafest.ru"


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


def test_parse_grist_bool(monkeypatch):
    bot = load_bot_module(monkeypatch)

    assert bot.parse_grist_bool(True) is True
    assert bot.parse_grist_bool(1) is True
    assert bot.parse_grist_bool(" yes ") is True
    assert bot.parse_grist_bool("off") is False
    assert bot.parse_grist_bool(None) is False


def test_prune_registration_times_removes_only_stale_entries(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.user_registration_times.clear()
    now = 10_000.0
    stale_time = now - (bot.REGISTRATION_RATE_LIMIT * 2) - 1
    fresh_time = now - 30
    borderline_time = now - (bot.REGISTRATION_RATE_LIMIT * 2)
    bot.user_registration_times.update({1: stale_time, 2: fresh_time, 3: borderline_time})

    bot.prune_registration_times(now)

    assert 1 not in bot.user_registration_times
    assert bot.user_registration_times[2] == fresh_time
    assert bot.user_registration_times[3] == borderline_time


def test_request_with_retries_retries_then_succeeds(monkeypatch):
    bot = load_bot_module(monkeypatch)

    class DummyClient:
        def __init__(self):
            self.calls = 0

        async def request(self, method, url, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise bot.httpx.RequestError("temporary")
            return FakeResponse(200, {"ok": True})

    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(bot.asyncio, "sleep", fake_sleep)

    client = DummyClient()
    response = asyncio.run(bot.request_with_retries(client, "GET", "https://example.test"))

    assert response.status_code == 200
    assert client.calls == 2
    assert sleeps == [0.5]


def test_request_with_retries_raises_after_last_attempt(monkeypatch):
    bot = load_bot_module(monkeypatch)

    class DummyClient:
        async def request(self, method, url, **kwargs):
            raise bot.httpx.RequestError("still failing")

    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(bot.asyncio, "sleep", fake_sleep)

    try:
        asyncio.run(bot.request_with_retries(DummyClient(), "GET", "https://example.test"))
        assert False, "expected RequestError"
    except bot.httpx.RequestError:
        pass

    assert sleeps == [0.5, 1.0]


def test_notify_owner_skips_when_owner_missing(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "OWNER_TELEGRAM_ID", None)

    context = DummyContext()

    asyncio.run(bot.notify_owner(context, "hello"))

    assert context.bot.sent == []


def test_notify_owner_logs_send_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "OWNER_TELEGRAM_ID", 77)

    errors = []

    async def fake_send_message(chat_id, text):
        raise RuntimeError("send failed")

    def fake_error(message, *args, **kwargs):
        errors.append(message % args if args else message)

    context = DummyContext()
    context.bot.send_message = fake_send_message
    monkeypatch.setattr(bot.logger, "error", fake_error)

    asyncio.run(bot.notify_owner(context, "hello"))

    assert errors
    assert "Failed to notify owner" in errors[0]


def test_update_grist_people_matrix_id_failed_status(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_person_row_id.clear()
    bot.grist_handle_to_person_row_id["alice"] = 21

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(500, {}, text="boom")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.update_grist_people_matrix_id("alice", "@alice:insomniafest.ru"))

    assert ok is False
    assert code == "PEOPLE_UPDATE_FAILED"


def test_update_grist_people_matrix_id_exception(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_person_row_id.clear()
    bot.grist_handle_to_person_row_id["alice"] = 21

    async def fake_request_with_retries(client, method, url, **kwargs):
        raise RuntimeError("patch failed")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.update_grist_people_matrix_id("alice", "@alice:insomniafest.ru"))

    assert ok is False
    assert code == "PEOPLE_UPDATE_EXCEPTION"


def test_get_synapse_registration_status_unknown_without_token(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    status = asyncio.run(bot.get_synapse_registration_status("alice"))

    assert status == "unknown"


def test_get_synapse_registration_status_unknown_on_unexpected_status(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(500, {}, text="oops")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    status = asyncio.run(bot.get_synapse_registration_status("alice"))

    assert status == "unknown"


def test_prepare_team_sync_target_error_paths(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_check_failed(handle):
        return False, False, None, {}

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_failed)
    result = asyncio.run(bot.prepare_team_sync_target("@alice"))
    assert result == ("", None, {}, "CHECK_FAILED")

    async def fake_not_eligible(handle):
        return False, True, None, {}

    monkeypatch.setattr(bot, "check_user_eligibility", fake_not_eligible)
    result = asyncio.run(bot.prepare_team_sync_target("@alice"))
    assert result == ("", None, {}, "NOT_ELIGIBLE")

    async def fake_eligible(handle):
        return True, True, "Alice", {}

    async def fake_get_status(username):
        return "unknown"

    monkeypatch.setattr(bot, "check_user_eligibility", fake_eligible)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_status)
    result = asyncio.run(bot.prepare_team_sync_target("@alice"))
    assert result == ("alice", "Alice", {}, "NOT_REGISTERED:unknown")

    async def fake_registered(username):
        return "registered"

    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_registered)
    result = asyncio.run(bot.prepare_team_sync_target("@alice"))
    assert result == ("alice", "Alice", {}, "NO_MEMBERSHIPS")


def test_split_and_build_team_sync_message(monkeypatch):
    bot = load_bot_module(monkeypatch)

    team_results = [
        {
            "team_name": "Alpha & Beta",
            "is_organizer": True,
            "room_id": "!room1:insomniafest.ru",
            "status": "already_joined",
        },
        {
            "team_name": "Gamma",
            "is_organizer": False,
            "room_id": None,
            "status": "joined",
        },
        {
            "team_name": "Delta",
            "is_organizer": False,
            "room_id": None,
            "status": "failed",
        },
    ]

    already_joined, newly_joined, failed = bot.split_team_sync_results(team_results)
    message = bot.build_team_sync_message(
        team_results,
        ["Alpha & Beta"],
        title="TITLE",
        already_title="ALREADY",
        newly_title="NEW",
        header_lines=["header"],
    )

    assert len(already_joined) == 1
    assert "Alpha &amp; Beta" in already_joined[0]
    assert len(newly_joined) == 1
    assert "Gamma" in newly_joined[0]
    assert failed == ["Delta"]
    assert "TITLE" in message
    assert "header" in message
    assert "ALREADY" in message
    assert "NEW" in message
    assert "Не удалось добавить в комнаты: Delta" in message
    assert "Не удалось выдать права администратора" in message


def test_is_hr_command_user_refreshes_cache(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.grist_handle_to_is_hr_now.clear()
    bot.grist_handle_to_team_memberships.clear()

    async def fake_sync_grist_cache(force_full=False):
        bot.grist_handle_to_is_hr_now["alice"] = True
        return True

    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    allowed = asyncio.run(bot.is_hr_command_user(DummyUpdate(user_id=2, username="alice")))

    assert allowed is True


def test_ops_sync_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_sync_grist_cache(force_full=False):
        return False

    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext()

    asyncio.run(bot.ops_sync(update, context))

    assert update.message.sent[-1]["text"] == "❌ Не удалось обновить кэш Гриста."


def test_ops_check_usage(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=[])

    asyncio.run(bot.ops_check(update, context))

    assert update.message.sent[-1]["text"] == "Использование: /hr_check <телеграм_ник>"


def test_ops_register_usage(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=[])

    asyncio.run(bot.ops_register(update, context))

    assert update.message.sent[-1]["text"] == "Использование: /hr_register <телеграм_ник>"


def test_ops_join_teams_not_registered(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_check_user_eligibility(handle):
        return True, True, "Alice", {1: True}

    async def fake_get_synapse_registration_status(username):
        return "unknown"

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_join_teams(update, context))

    assert "пока не зарегистрирован в Matrix" in update.message.sent[-1]["text"]


def test_set_synapse_display_name_no_token(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    ok = asyncio.run(bot.set_synapse_display_name("alice", "Alice"))

    assert ok is False


def test_set_synapse_display_name_request_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok = asyncio.run(bot.set_synapse_display_name("alice", "Alice"))

    assert ok is False


def test_join_user_to_room_exception_returns_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    status = asyncio.run(bot.join_user_to_room("alice", "!room:insomniafest.ru"))

    assert status == "failed"


def test_post_init_raises_when_initial_sync_fails(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_check_synapse_admin_token():
        return True, None

    async def fake_sync_grist_cache(force_full=False):
        return False

    monkeypatch.setattr(bot, "OWNER_TELEGRAM_ID", None)
    monkeypatch.setattr(bot, "check_synapse_admin_token", fake_check_synapse_admin_token)
    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    class DummyApplication:
        def __init__(self):
            self.bot = DummyBot()

    try:
        asyncio.run(bot.post_init(DummyApplication()))
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "Initial Grist cache sync failed" in str(exc)


def test_post_init_notifies_owner_on_startup(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "OWNER_TELEGRAM_ID", 77)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")
    bot.grist_handle_to_record_id.clear()
    bot.grist_handle_to_record_id["alice"] = 1
    bot.grist_max_record_id = 55

    async def fake_check_synapse_admin_token():
        return True, None

    async def fake_sync_grist_cache(force_full=False):
        return True

    class DummyApplication:
        def __init__(self):
            self.bot = DummyBot()

    monkeypatch.setattr(bot, "check_synapse_admin_token", fake_check_synapse_admin_token)
    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    application = DummyApplication()
    asyncio.run(bot.post_init(application))

    assert len(application.bot.sent) == 1
    assert application.bot.sent[0]["chat_id"] == 77
    assert "Бот запущен" in application.bot.sent[0]["text"]
    assert "Токен Synapse действителен" in application.bot.sent[0]["text"]


def test_reset_password_eligibility_check_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)
    base_update = DummyUpdate(user_id=42, username="alice")
    update = DummyCallbackUpdate(user_id=42, username="alice", message=base_update.message)
    context = DummyContext()

    bot.user_registration_times.clear()
    bot.password_reset_confirmations.clear()
    bot.password_reset_confirmations[42] = 1_000_000.0
    monkeypatch.setattr(bot.time, "time", lambda: 1_000_000.0)

    async def fake_check_registration_eligibility(username):
        return False, False, None, {}

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)

    asyncio.run(bot.reset_password_confirm_callback(update, context))

    assert len(base_update.message.sent) == 2
    assert "Не удалось проверить данные" in base_update.message.sent[1]["text"]


def test_reset_password_not_eligible(monkeypatch):
    bot = load_bot_module(monkeypatch)
    base_update = DummyUpdate(user_id=42, username="alice")
    update = DummyCallbackUpdate(user_id=42, username="alice", message=base_update.message)
    context = DummyContext()

    bot.user_registration_times.clear()
    bot.password_reset_confirmations.clear()
    bot.password_reset_confirmations[42] = 1_000_000.0
    monkeypatch.setattr(bot.time, "time", lambda: 1_000_000.0)

    async def fake_check_registration_eligibility(username):
        return False, True, None, {}

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)

    asyncio.run(bot.reset_password_confirm_callback(update, context))

    assert len(base_update.message.sent) == 2
    assert "не найден в таблице people" in base_update.message.sent[1]["text"].lower()


def test_reset_password_token_missing(monkeypatch):
    bot = load_bot_module(monkeypatch)
    base_update = DummyUpdate(user_id=42, username="alice")
    update = DummyCallbackUpdate(user_id=42, username="alice", message=base_update.message)
    context = DummyContext()

    bot.user_registration_times.clear()
    bot.password_reset_confirmations.clear()
    bot.password_reset_confirmations[42] = 1_000_000.0
    monkeypatch.setattr(bot.time, "time", lambda: 1_000_000.0)

    async def fake_check_registration_eligibility(username):
        return True, True, "Alice", {72: False}

    async def fake_reset_synapse_password(username, password):
        return False, "RESET_TOKEN_MISSING"

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
    monkeypatch.setattr(bot, "reset_synapse_password", fake_reset_synapse_password)

    asyncio.run(bot.reset_password_confirm_callback(update, context))

    assert "временно недоступен" in base_update.message.sent[-1]["text"].lower()


def test_reset_password_failure_notifies_owner(monkeypatch):
    bot = load_bot_module(monkeypatch)
    base_update = DummyUpdate(user_id=42, username="alice")
    update = DummyCallbackUpdate(user_id=42, username="alice", message=base_update.message)
    context = DummyContext()

    bot.user_registration_times.clear()
    bot.password_reset_confirmations.clear()
    bot.password_reset_confirmations[42] = 1_000_000.0
    monkeypatch.setattr(bot.time, "time", lambda: 1_000_000.0)
    notified = []

    async def fake_check_registration_eligibility(username):
        return True, True, "Alice", {72: False}

    async def fake_reset_synapse_password(username, password):
        return False, "RESET_FAILED"

    async def fake_notify_owner(context_obj, message):
        notified.append(message)

    monkeypatch.setattr(bot, "check_registration_eligibility", fake_check_registration_eligibility)
    monkeypatch.setattr(bot, "reset_synapse_password", fake_reset_synapse_password)
    monkeypatch.setattr(bot, "notify_owner", fake_notify_owner)

    asyncio.run(bot.reset_password_confirm_callback(update, context))

    assert "Не удалось сбросить пароль" in base_update.message.sent[-1]["text"]
    assert len(notified) == 1
    assert "reset_error=RESET_FAILED" in notified[0]


def test_my_teams_check_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_prepare_team_sync_target(handle):
        return "", None, {}, "CHECK_FAILED"

    monkeypatch.setattr(bot, "prepare_team_sync_target", fake_prepare_team_sync_target)

    update = DummyUpdate(user_id=10, username="alice")
    context = DummyContext()

    asyncio.run(bot.join_teams(update, context))

    assert "Не удалось проверить данные регистрации" in update.message.sent[-1]["text"]


def test_my_teams_no_memberships(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_prepare_team_sync_target(handle):
        return "alice", "Alice", {}, "NO_MEMBERSHIPS"

    monkeypatch.setattr(bot, "prepare_team_sync_target", fake_prepare_team_sync_target)

    update = DummyUpdate(user_id=10, username="alice")
    context = DummyContext()

    asyncio.run(bot.join_teams(update, context))

    assert "Команды в Участиях 2026 для вас не найдены" in update.message.sent[-1]["text"]


def test_ops_check_handles_check_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return False, False, None, None, None

    async def fake_check_user_eligibility(handle):
        return False, False, None, {}

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_check(update, context))

    text = update.message.sent[-1]["text"]
    assert "Найден в Человеках: не удалось проверить" in text
    assert "Участия 2026: не удалось проверить" in text
    assert "👥 Команды: не удалось определить" in text


def test_ops_register_reports_reactivation_failure(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return True, True, 21, "Alice", True

    async def fake_check_user_eligibility(handle):
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        return False, "M_USER_IN_USE"

    async def fake_reactivate_synapse_user(username, password):
        return False, "BROKEN"

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "reactivate_synapse_user", fake_reactivate_synapse_user)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_register(update, context))

    assert "Не удалось реактивировать аккаунт alice: BROKEN" in update.message.sent[-1]["text"]


def test_ops_check_not_eligible(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return False, True, None, None, None

    async def fake_check_user_eligibility(handle):
        return False, True, None, {}

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@ghost"])

    asyncio.run(bot.ops_check(update, context))

    text = update.message.sent[-1]["text"]
    assert "Найден в Человеках: не найден" in text
    assert "Участия 2026: не найден" in text
    assert "👥 Команды: не указаны" in text


def test_ops_check_reports_no_memberships(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return True, True, 21, "Alice", True

    async def fake_check_user_eligibility(handle):
        return True, True, "Alice", {}

    async def fake_get_synapse_registration_status(username):
        return "unknown"

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_check(update, context))

    text = update.message.sent[-1]["text"]
    assert "ID в Человеках: 21" in text
    assert "Найден в Человеках: найден" in text
    assert "Участия 2026: найден" in text
    assert "Регистрация в Matrix: не удалось определить (@alice:insomniafest.ru)" in text
    assert "Комната организаторов: нет" in text
    assert "👥 Команды: не указаны" in text


def test_ops_check_reports_people_without_participations(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        assert handle == "@alice"
        return True, True, 21, "Alice", True

    async def fake_check_user_eligibility(handle):
        assert handle == "@alice"
        return False, True, None, {}

    async def fake_get_synapse_registration_status(username):
        assert username == "alice"
        return "not_registered"

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_check(update, context))

    text = update.message.sent[-1]["text"]
    assert "ID в Человеках: 21" in text
    assert "Найден в Человеках: найден" in text
    assert "Черный список: нет" in text
    assert "Участия 2026: не найден" in text
    assert "Регистрация в Matrix: не зарегистрирован (@alice:insomniafest.ru)" in text
    assert "👥 Команды: не указаны" in text


def test_ops_register_check_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return False, False, None, None, None

    async def fake_check_user_eligibility(handle):
        return False, False, None, {}

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_register(update, context))

    text = update.message.sent[-1]["text"]
    assert "❌ Не удалось проверить данные участника" in text
    assert "people=не удалось проверить" in text
    assert "участия_2026=не удалось проверить" in text


def test_ops_register_not_eligible(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return False, True, None, None, None

    async def fake_check_user_eligibility(handle):
        return False, True, None, {}

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_register(update, context))

    text = update.message.sent[-1]["text"]
    assert "❌ Человек не найден в таблице People или находится в черном списке" in text
    assert "people=не найден" in text
    assert "участия_2026=не найден" in text


def test_ops_register_registration_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return True, True, 21, "Alice", True

    async def fake_check_user_eligibility(handle):
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        return False, "REGISTER_FAILED"

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_register(update, context))

    assert update.message.sent[-1]["text"] == "❌ Не удалось зарегистрировать alice: REGISTER_FAILED"


def test_ops_register_account_active_path_reports_existing(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return True, True, 21, "Alice", True

    async def fake_check_user_eligibility(handle):
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        return False, "M_USER_IN_USE"

    async def fake_reactivate_synapse_user(username, password):
        return False, "ACCOUNT_ACTIVE"

    async def fake_set_synapse_display_name(username, display_name):
        return True

    async def fake_join_user_to_rooms(username, rooms):
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        return True, [], []

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "reactivate_synapse_user", fake_reactivate_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_register(update, context))

    text = update.message.sent[-1]["text"]
    assert "Person Row ID: 21" in text
    assert "Аккаунт создан: нет" in text
    assert "Аккаунт реактивирован: нет" in text
    assert "временный_пароль=" not in text
    assert "Добавление в командные комнаты: да" in text

def test_ops_register_reactivation_token_missing_path_reports_existing(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_get_people_status_details(handle):
        return True, True, 21, None, True

    async def fake_check_user_eligibility(handle):
        return True, True, None, {72: True}

    async def fake_register_synapse_user(username, password):
        return False, "M_USER_IN_USE"

    async def fake_reactivate_synapse_user(username, password):
        return False, "REACTIVATION_TOKEN_MISSING"

    async def fake_join_user_to_rooms(username, rooms):
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        return True, [], []

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "reactivate_synapse_user", fake_reactivate_synapse_user)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_register(update, context))

    text = update.message.sent[-1]["text"]
    assert "Имя: -" in text
    assert "Person Row ID: 21" in text
    assert "Аккаунт создан: нет" in text
    assert "Аккаунт реактивирован: нет" in text
    assert "временный_пароль=" not in text


def test_ops_register_fake_tg_uses_stripped_localpart_and_cleans_grist(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)
    captured = {}

    async def fake_get_people_status_details(handle):
        captured["people_handle"] = handle
        return True, True, 21, "Alice", True

    async def fake_check_user_eligibility(handle):
        captured["eligibility_handle"] = handle
        return True, True, "Alice", {72: False}

    async def fake_register_synapse_user(username, password):
        captured["register_username"] = username
        return True, None

    async def fake_set_synapse_display_name(username, display_name):
        return True

    async def fake_join_user_to_rooms(username, rooms):
        captured["join_username"] = username
        return True, []

    async def fake_join_user_to_team_rooms(username, memberships):
        return True, [], []

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "register_synapse_user", fake_register_synapse_user)
    monkeypatch.setattr(bot, "set_synapse_display_name", fake_set_synapse_display_name)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)
    monkeypatch.setattr(bot, "join_user_to_team_rooms", fake_join_user_to_team_rooms)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@$alice"])

    asyncio.run(bot.ops_register(update, context))

    assert captured["people_handle"] == "@$alice"
    assert captured["eligibility_handle"] == "@$alice"
    assert captured["register_username"] == "alice"
    assert captured["join_username"] == "alice"


def test_ops_join_teams_check_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_check_user_eligibility(handle):
        return False, False, None, {}

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_join_teams(update, context))

    assert update.message.sent[-1]["text"] == "❌ Не удалось проверить данные участника"


def test_ops_join_teams_not_eligible(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_check_user_eligibility(handle):
        return False, True, None, {}

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@ghost"])

    asyncio.run(bot.ops_join_teams(update, context))

    assert update.message.sent[-1]["text"] == "❌ Участник не найден: @ghost"


def test_ops_join_teams_not_eligible_with_edited_message(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_check_user_eligibility(handle):
        return False, True, None, {}

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)

    update = DummyUpdate(user_id=1, username="admin", is_edited=True)
    context = DummyContext(args=["@ghost"])

    asyncio.run(bot.ops_join_teams(update, context))

    assert update.edited_message.sent[-1]["text"] == "❌ Участник не найден: @ghost"


def test_ops_join_teams_no_memberships(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_check_user_eligibility(handle):
        return True, True, "Alice", {}

    async def fake_get_synapse_registration_status(username):
        return "registered"

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice"])

    asyncio.run(bot.ops_join_teams(update, context))

    assert update.message.sent[-1]["text"] == "ℹ️ Для @alice не найдено команд в Участиях 2026."


def test_check_user_eligibility_matrix_id_lookup(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_person_row_to_person_name.clear()
    bot.grist_person_row_to_team_memberships.clear()
    bot.grist_matrix_id_to_person_row_id.clear()

    bot.grist_person_row_to_person_name[21] = "Alice"
    bot.grist_person_row_to_team_memberships[21] = {72: True, 73: False}
    bot.grist_matrix_id_to_person_row_id["@alice:insomniafest.ru"] = 21

    async def fake_sync_grist_cache(force_full=False):
        return True

    async def fake_sync_grist_people_matrix_cache(force_full=False):
        return True

    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)
    monkeypatch.setattr(bot, "sync_grist_people_matrix_cache", fake_sync_grist_people_matrix_cache)

    eligible, check_ok, person_name, memberships = asyncio.run(
        bot.check_user_eligibility("@alice:insomniafest.ru")
    )

    assert eligible is True
    assert check_ok is True
    assert person_name == "Alice"
    assert memberships == {72: True, 73: False}


def test_prepare_team_sync_target_with_matrix_id_uses_localpart(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_check_user_eligibility(handle):
        return True, True, "Alice", {72: True}

    captured = {}

    async def fake_get_synapse_registration_status(username):
        captured["username"] = username
        return "registered"

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)

    normalized_handle, person_name, memberships, error_code = asyncio.run(
        bot.prepare_team_sync_target("@alice:insomniafest.ru")
    )

    assert normalized_handle == "alice"
    assert person_name == "Alice"
    assert memberships == {72: True}
    assert error_code is None
    assert captured["username"] == "alice"


def test_ops_check_accepts_matrix_id(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)
    bot.grist_team_id_to_name.clear()
    bot.grist_team_id_to_name[72] = "Точка сборки"

    async def fake_get_people_status_details(handle):
        assert handle == "@alice:insomniafest.ru"
        return True, True, 21, "Alice", True

    async def fake_check_user_eligibility(handle):
        assert handle == "@alice:insomniafest.ru"
        return True, True, "Alice", {72: True}

    async def fake_get_synapse_registration_status(username):
        assert username == "alice"
        return "registered"

    monkeypatch.setattr(bot, "get_people_status_details", fake_get_people_status_details)
    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice:insomniafest.ru"])

    asyncio.run(bot.ops_check(update, context))

    text = update.message.sent[-1]["text"]
    assert "Matrix ID: @alice:insomniafest.ru" in text
    assert "Имя: Alice" in text
    assert "ID в Человеках: 21" in text
    assert "Найден в Человеках: найден" in text
    assert "Участия 2026: найден" in text
    assert "Регистрация в Matrix: уже зарегистрирован (@alice:insomniafest.ru)" in text
    assert "Комната организаторов: да" in text
    assert "👥 Команды:" in text
    assert "• Точка сборки — организатор" in text


def test_ops_join_teams_accepts_matrix_id(monkeypatch):
    bot = load_bot_module(monkeypatch)
    captured = {}

    bot.ADMIN_TELEGRAM_IDS.clear()
    bot.ADMIN_TELEGRAM_IDS.add(1)

    async def fake_check_user_eligibility(handle):
        assert handle == "@alice:insomniafest.ru"
        return True, True, "Alice", {72: True}

    async def fake_get_synapse_registration_status(username):
        assert username == "alice"
        return "registered"

    async def fake_sync_user_to_team_rooms_detailed(username, memberships):
        assert username == "alice"
        assert memberships == {72: True}
        return [
            {
                "team_id": 72,
                "team_name": "Точка сборки",
                "is_organizer": True,
                "room_id": "!team72:insomniafest.ru",
                "status": "joined",
            }
        ], []

    async def fake_join_user_to_rooms(username, rooms):
        captured["orgs_join_username"] = username
        captured["orgs_join_rooms"] = list(rooms)
        return True, []

    monkeypatch.setattr(bot, "check_user_eligibility", fake_check_user_eligibility)
    monkeypatch.setattr(bot, "get_synapse_registration_status", fake_get_synapse_registration_status)
    monkeypatch.setattr(bot, "sync_user_to_team_rooms_detailed", fake_sync_user_to_team_rooms_detailed)
    monkeypatch.setattr(bot, "join_user_to_rooms", fake_join_user_to_rooms)

    update = DummyUpdate(user_id=1, username="admin")
    context = DummyContext(args=["@alice:insomniafest.ru"])

    asyncio.run(bot.ops_join_teams(update, context))

    text = update.message.sent[-1]["text"]
    assert "Пользователь: @alice" in text
    assert "Имя: Alice" in text
    assert captured["orgs_join_username"] == "alice"
    assert captured["orgs_join_rooms"] == [bot.ORGS_ROOM]


def test_check_user_eligibility_empty_handle(monkeypatch):
    bot = load_bot_module(monkeypatch)

    eligible, check_ok, person_name, memberships = asyncio.run(bot.check_user_eligibility(""))

    assert eligible is False
    assert check_ok is True
    assert person_name is None
    assert memberships == {}


def test_check_user_eligibility_exception(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_sync_grist_cache(force_full=False):
        raise RuntimeError("boom")

    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    eligible, check_ok, person_name, memberships = asyncio.run(bot.check_user_eligibility("alice"))

    assert eligible is False
    assert check_ok is False
    assert person_name is None
    assert memberships == {}


def test_check_user_eligibility_uses_stale_cache_when_sync_fails(monkeypatch):
    bot = load_bot_module(monkeypatch)

    bot.grist_handle_to_record_id.clear()
    bot.grist_handle_to_person_name.clear()
    bot.grist_handle_to_team_memberships.clear()
    bot.grist_handle_to_record_id["alice"] = 123
    bot.grist_handle_to_person_name["alice"] = "Alice"
    bot.grist_handle_to_team_memberships["alice"] = {7: True}

    async def fake_sync_grist_cache(force_full=False):
        return False

    monkeypatch.setattr(bot, "sync_grist_cache", fake_sync_grist_cache)

    eligible, check_ok, person_name, memberships = asyncio.run(bot.check_user_eligibility("alice"))

    assert eligible is True
    assert check_ok is True
    assert person_name == "Alice"
    assert memberships == {7: True}


def test_register_synapse_user_nonce_request_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(500, {}, text="no nonce")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.register_synapse_user("alice", "pwd"))

    assert ok is False
    assert code == "NONCE_REQUEST_FAILED"


def test_register_synapse_user_register_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(200, {"nonce": "abc"})

    async def fake_post(self, url, **kwargs):
        return FakeResponse(500, {}, text="broken")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(bot.httpx.AsyncClient, "post", fake_post)

    ok, code = asyncio.run(bot.register_synapse_user("alice", "pwd"))

    assert ok is False
    assert code == "REGISTER_FAILED"


def test_register_synapse_user_stale_nonce_exhausted(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(200, {"nonce": "abc"})

    async def fake_post(self, url, **kwargs):
        return FakeResponse(400, {"errcode": "M_UNKNOWN"}, text="unrecognised nonce")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(bot.httpx.AsyncClient, "post", fake_post)

    ok, code = asyncio.run(bot.register_synapse_user("alice", "pwd"))

    assert ok is False
    assert code == "REGISTER_FAILED"


def test_reactivate_synapse_user_no_token(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    ok, code = asyncio.run(bot.reactivate_synapse_user("alice", "pwd"))

    assert ok is False
    assert code == "REACTIVATION_TOKEN_MISSING"


def test_reactivate_synapse_user_lookup_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(404, {}, text="not found")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.reactivate_synapse_user("alice", "pwd"))

    assert ok is False
    assert code == "USER_LOOKUP_FAILED"


def test_reactivate_synapse_user_reactivation_failed(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    responses = [
        FakeResponse(200, {"deactivated": True}),
        FakeResponse(500, {}, text="broken"),
    ]

    async def fake_request_with_retries(client, method, url, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.reactivate_synapse_user("alice", "pwd"))

    assert ok is False
    assert code == "REACTIVATION_FAILED"


def test_reset_synapse_password_exception(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok, code = asyncio.run(bot.reset_synapse_password("alice", "pwd"))

    assert ok is False
    assert code == "RESET_EXCEPTION"


def test_set_synapse_display_name_empty_is_noop(monkeypatch):
    bot = load_bot_module(monkeypatch)

    ok = asyncio.run(bot.set_synapse_display_name("alice", ""))

    assert ok is True


def test_set_synapse_display_name_failed_status(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(500, {}, text="broken")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok = asyncio.run(bot.set_synapse_display_name("alice", "Alice"))

    assert ok is False


def test_resolve_room_alias_non_200(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(404, {}, text="missing")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    room_id = asyncio.run(bot.resolve_room_alias("#team-1:insomniafest.ru"))

    assert room_id is None


def test_resolve_room_alias_exception(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    room_id = asyncio.run(bot.resolve_room_alias("#team-1:insomniafest.ru"))

    assert room_id is None


def test_create_team_room_no_token(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    room_id = asyncio.run(bot.create_team_room(1, "Alpha"))

    assert room_id is None


def test_create_team_room_success(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(200, {"room_id": "!new:insomniafest.ru"})

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    room_id = asyncio.run(bot.create_team_room(1, "Alpha"))

    assert room_id == "!new:insomniafest.ru"


def test_create_team_room_failure_without_fallback(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(500, {}, text="broken")

    async def fake_resolve_room_alias(alias):
        return None

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(bot, "resolve_room_alias", fake_resolve_room_alias)

    room_id = asyncio.run(bot.create_team_room(1, "Alpha"))

    assert room_id is None


def test_ensure_team_room_returns_existing(monkeypatch):
    bot = load_bot_module(monkeypatch)

    async def fake_resolve_room_alias(alias):
        return "!existing:insomniafest.ru"

    async def fake_create_team_room(team_id, team_name):
        raise AssertionError("create_team_room should not be called")

    monkeypatch.setattr(bot, "resolve_room_alias", fake_resolve_room_alias)
    monkeypatch.setattr(bot, "create_team_room", fake_create_team_room)

    room_id = asyncio.run(bot.ensure_team_room(1, "Alpha"))

    assert room_id == "!existing:insomniafest.ru"


def test_set_room_moderator_no_token(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    ok = asyncio.run(bot.set_room_moderator("!room:insomniafest.ru", "@alice:insomniafest.ru"))

    assert ok is False


def test_set_room_moderator_failed_put(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    responses = [
        FakeResponse(200, {"users": {}}),
        FakeResponse(500, {}, text="broken"),
    ]

    async def fake_request_with_retries(client, method, url, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    ok = asyncio.run(bot.set_room_moderator("!room:insomniafest.ru", "@alice:insomniafest.ru"))

    assert ok is False


def test_get_room_parent_spaces_no_token(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", None)

    spaces = asyncio.run(bot.get_room_parent_spaces("!room:insomniafest.ru"))

    assert spaces == []


def test_get_room_parent_spaces_non_200(monkeypatch):
    bot = load_bot_module(monkeypatch)
    monkeypatch.setattr(bot, "SYNAPSE_ADMIN_ACCESS_TOKEN", "token")

    async def fake_request_with_retries(client, method, url, **kwargs):
        return FakeResponse(500, {}, text="broken")

    monkeypatch.setattr(bot, "request_with_retries", fake_request_with_retries)

    spaces = asyncio.run(bot.get_room_parent_spaces("!room:insomniafest.ru"))

    assert spaces == []
