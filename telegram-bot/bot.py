from __future__ import annotations

import os
import logging
import asyncio
import html
import httpx
import hmac
import hashlib
import secrets
import time
from urllib.parse import quote
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import NetworkError
from telegram.ext import Application, CommandHandler, ContextTypes

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
SYNAPSE_ADMIN_TOKEN = os.getenv('SYNAPSE_REGISTRATION_SHARED_SECRET')
SYNAPSE_ADMIN_ACCESS_TOKEN = os.getenv('SYNAPSE_ADMIN_ACCESS_TOKEN')
SYNAPSE_API_URL = "https://synapse.insomniafest.ru"
SYNAPSE_SERVER_NAME = os.getenv('SYNAPSE_SERVER_NAME', 'insomniafest.ru')
ELEMENT_URL = "https://chat.insomniafest.ru"
HELP_URL = "https://chat.insomniafest.ru/help"
AUTO_JOIN_ROOMS = (
    '#announcements:insomniafest.ru',
    '#general:insomniafest.ru',
)
ORGS_ROOM = '#orgs:insomniafest.ru'
GRIST_DOC_ID = "mhwDM83vLmT3"
GRIST_TABLE_ID = "Participations"
GRIST_TEAMS_TABLE_ID = "Teams"
GRIST_PEOPLE_TABLE_ID = "People"
TEAM_ROOM_ORGANIZER_POWER_LEVEL = 100

GRIST_API_KEY = os.getenv('GRIST_API_KEY')
OWNER_TELEGRAM_ID_RAW = os.getenv('OWNER_TELEGRAM_ID')

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set")
if not SYNAPSE_ADMIN_TOKEN:
    raise ValueError("SYNAPSE_REGISTRATION_SHARED_SECRET environment variable not set")
if not GRIST_API_KEY:
    raise ValueError("GRIST_API_KEY environment variable not set")

OWNER_TELEGRAM_ID = None
if OWNER_TELEGRAM_ID_RAW:
    try:
        OWNER_TELEGRAM_ID = int(OWNER_TELEGRAM_ID_RAW)
    except ValueError:
        logger.error("OWNER_TELEGRAM_ID must be a valid integer Telegram chat ID")

ADMIN_TELEGRAM_IDS = set()
if OWNER_TELEGRAM_ID is not None:
    ADMIN_TELEGRAM_IDS.add(OWNER_TELEGRAM_ID)

# Rate limiting: track last registration attempt per user (user_id -> timestamp)
REGISTRATION_RATE_LIMIT = 300  # 5 minutes in seconds
user_registration_times = {}

# HTTP settings for external APIs
HTTP_TIMEOUT = httpx.Timeout(timeout=15.0, connect=5.0)
HTTP_RETRIES = 2

# Grist eligibility cache
GRIST_ALLOWED_STATUS_CODES = ("PLANNED", "STARTED", "COMPLETE")
GRIST_CACHE_FULL_SYNC_INTERVAL = 600  # seconds
grist_cache_lock = asyncio.Lock()
grist_handle_to_record_id = {}
grist_handle_to_person_name = {}
grist_handle_to_team_memberships = {}
grist_handle_to_is_hr_now = {}
grist_handle_to_person_row_id = {}
grist_person_row_to_person_name = {}
grist_person_row_to_team_memberships = {}
grist_matrix_id_to_person_row_id = {}
grist_team_id_to_name = {}
grist_max_record_id = 0
grist_last_full_sync = 0.0
grist_people_last_full_sync = 0.0

def prune_registration_times(now: float) -> None:
    """Drop old rate-limit entries to keep memory usage bounded."""
    cutoff = now - (REGISTRATION_RATE_LIMIT * 2)
    stale_user_ids = [uid for uid, ts in user_registration_times.items() if ts < cutoff]
    for uid in stale_user_ids:
        user_registration_times.pop(uid, None)


async def request_with_retries(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs,
) -> httpx.Response:
    """Retry transient network failures with exponential backoff."""
    for attempt in range(HTTP_RETRIES + 1):
        try:
            return await client.request(method, url, **kwargs)
        except httpx.RequestError as e:
            if attempt == HTTP_RETRIES:
                raise
            backoff_seconds = 0.5 * (2 ** attempt)
            logger.warning(
                f"HTTP request failed ({method} {url}), retry {attempt + 1}/{HTTP_RETRIES}: {e}"
            )
            await asyncio.sleep(backoff_seconds)


def normalize_telegram_handle(handle) -> str:
    """Normalize Telegram handles for case-insensitive matching."""
    if isinstance(handle, (list, tuple)):
        for candidate in handle:
            normalized = normalize_telegram_handle(candidate)
            if normalized:
                return normalized
        return ""

    if not isinstance(handle, str):
        return ""

    return handle.strip().lstrip('@').lower()


def is_fake_telegram_handle(handle: str) -> bool:
    """Treat handles with leading underscores as fake HR placeholders."""
    return isinstance(handle, str) and handle.startswith('_')


def registration_localpart_from_handle(handle: str) -> str:
    """Build Matrix localpart from Telegram handle, stripping fake leading underscores."""
    normalized = normalize_telegram_handle(handle)
    if not normalized:
        return ""

    stripped = normalized.lstrip('_')
    return stripped or normalized


def is_matrix_id(value) -> bool:
    """Return True when value looks like a full Matrix ID (@localpart:server)."""
    if not isinstance(value, str):
        return False
    text = value.strip()
    return text.startswith('@') and ':' in text


def normalize_matrix_id(value) -> str:
    """Normalize Matrix ID for case-insensitive matching."""
    if not isinstance(value, str):
        return ""
    text = value.strip().lower()
    if not text.startswith('@'):
        return ""
    if ':' not in text:
        return ""
    return text


def matrix_localpart_from_id(matrix_id: str) -> str:
    """Extract Matrix localpart from full Matrix ID."""
    normalized = normalize_matrix_id(matrix_id)
    if not normalized:
        return ""
    localpart, _, _ = normalized[1:].partition(':')
    return localpart


def parse_grist_ref_id(value) -> int | None:
    """Parse Grist reference cell that may be int, numeric string, or list/tuple."""
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_grist_bool(value) -> bool:
    """Parse boolean-ish values coming from Grist fields."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "yes", "y", "on"}
    return False


async def fetch_grist_records_via_records_api() -> list:
    """Fetch eligible records using Grist records API."""
    url = f"https://grist.insomniafest.ru/api/docs/{GRIST_DOC_ID}/tables/{GRIST_TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {GRIST_API_KEY}",
        "Content-Type": "application/json",
    }
    params = {
        "filter": (
            "{"
            '"year":[2026],'
            '"status_code":["PLANNED","STARTED","COMPLETE"]'
            "}"
        )
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await request_with_retries(
            client,
            "GET",
            url,
            params=params,
            headers=headers,
        )

    if response.status_code != 200:
        raise RuntimeError(f"Grist records API error: {response.status_code} {response.text}")

    data = response.json()
    return data.get("records", [])


async def fetch_grist_teams_via_records_api() -> list:
    """Fetch teams from Grist Teams table."""
    url = f"https://grist.insomniafest.ru/api/docs/{GRIST_DOC_ID}/tables/{GRIST_TEAMS_TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {GRIST_API_KEY}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await request_with_retries(
            client,
            "GET",
            url,
            headers=headers,
        )

    if response.status_code != 200:
        raise RuntimeError(f"Grist teams API error: {response.status_code} {response.text}")

    data = response.json()
    return data.get("records", [])


async def fetch_grist_people_via_records_api() -> list:
    """Fetch people records from Grist People table."""
    url = f"https://grist.insomniafest.ru/api/docs/{GRIST_DOC_ID}/tables/{GRIST_PEOPLE_TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {GRIST_API_KEY}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await request_with_retries(
            client,
            "GET",
            url,
            headers=headers,
        )

    if response.status_code != 200:
        raise RuntimeError(f"Grist people API error: {response.status_code} {response.text}")

    data = response.json()
    return data.get("records", [])


async def sync_grist_people_matrix_cache(force_full: bool = False) -> bool:
    """Sync Matrix ID -> person row mapping from Grist People table."""
    global grist_people_last_full_sync

    async with grist_cache_lock:
        now = time.time()
        if (
            not force_full
            and grist_matrix_id_to_person_row_id
            and (now - grist_people_last_full_sync) < GRIST_CACHE_FULL_SYNC_INTERVAL
        ):
            return True

        try:
            people_records = await fetch_grist_people_via_records_api()
        except Exception as e:
            logger.warning(f"Failed to sync Grist people cache: {e}")
            return False

        grist_matrix_id_to_person_row_id.clear()

        for record in people_records:
            fields = record.get("fields", {})
            person_row_id = fields.get("id")
            if person_row_id is None:
                person_row_id = record.get("id")
            matrix_id = normalize_matrix_id(fields.get("matrix_id"))

            person_row_id = parse_grist_ref_id(person_row_id)
            if person_row_id is None or not matrix_id:
                continue

            grist_matrix_id_to_person_row_id[matrix_id] = person_row_id

        grist_people_last_full_sync = now
        return True


async def sync_grist_cache(force_full: bool = False) -> bool:
    """Sync eligibility cache from Grist records API."""
    global grist_max_record_id, grist_last_full_sync

    async with grist_cache_lock:
        now = time.time()
        if (
            not force_full
            and grist_handle_to_record_id
            and (now - grist_last_full_sync) < GRIST_CACHE_FULL_SYNC_INTERVAL
        ):
            return True

        try:
            records = await fetch_grist_records_via_records_api()
        except Exception as e:
            logger.error(f"Failed to sync Grist cache via records API: {e}")
            return False

        try:
            team_records = await fetch_grist_teams_via_records_api()
        except Exception as e:
            logger.warning(f"Failed to sync Grist teams cache: {e}")
            team_records = []

        grist_handle_to_record_id.clear()
        grist_handle_to_person_name.clear()
        grist_handle_to_team_memberships.clear()
        grist_handle_to_is_hr_now.clear()
        grist_handle_to_person_row_id.clear()
        grist_person_row_to_person_name.clear()
        grist_person_row_to_team_memberships.clear()
        grist_team_id_to_name.clear()
        grist_max_record_id = 0

        for team_record in team_records:
            team_id = team_record.get("id")
            team_fields = team_record.get("fields", {})
            team_name = team_fields.get("team_name")

            try:
                team_id = int(team_id)
            except (TypeError, ValueError):
                continue

            if isinstance(team_name, str) and team_name.strip():
                grist_team_id_to_name[team_id] = team_name.strip()

        for record in records:
            fields = record.get("fields", {})
            record_id = fields.get("id")
            if record_id is None:
                record_id = record.get("id")
            telegram2 = fields.get("Telegram2")
            person_name = fields.get("person_name")
            team_id = parse_grist_ref_id(fields.get("team"))
            role_code = fields.get("role_code")
            is_hr_now = parse_grist_bool(fields.get("isHR_Now"))
            person_row_id = parse_grist_ref_id(fields.get("person_row_id"))
            if person_row_id is None:
                person_row_id = parse_grist_ref_id(fields.get("person"))

            try:
                record_id = int(record_id)
            except (TypeError, ValueError):
                continue

            if person_row_id is not None:
                if isinstance(person_name, str) and person_name.strip():
                    grist_person_row_to_person_name[person_row_id] = person_name.strip()

                try:
                    if team_id is not None:
                        row_memberships = grist_person_row_to_team_memberships.setdefault(person_row_id, {})
                        is_organizer = (
                            isinstance(role_code, str)
                            and role_code.strip().upper() == "ORGANIZER"
                        )
                        row_memberships[team_id] = row_memberships.get(team_id, False) or is_organizer
                except (TypeError, ValueError):
                    pass

            normalized = normalize_telegram_handle(telegram2)
            if normalized:
                grist_handle_to_record_id[normalized] = record_id
                if isinstance(person_name, str) and person_name.strip():
                    grist_handle_to_person_name[normalized] = person_name.strip()
                else:
                    grist_handle_to_person_name.pop(normalized, None)

                if is_hr_now:
                    grist_handle_to_is_hr_now[normalized] = True

                if person_row_id is not None:
                    grist_handle_to_person_row_id[normalized] = person_row_id

                try:
                    if team_id is None:
                        raise ValueError("empty team ref")
                    memberships = grist_handle_to_team_memberships.setdefault(normalized, {})
                    is_organizer = (
                        isinstance(role_code, str)
                        and role_code.strip().upper() == "ORGANIZER"
                    )
                    memberships[team_id] = memberships.get(team_id, False) or is_organizer
                except (TypeError, ValueError):
                    pass

            if record_id > grist_max_record_id:
                grist_max_record_id = record_id

        grist_last_full_sync = now
        logger.info(
            f"Grist sync complete: {len(grist_handle_to_record_id)} handles, "
            f"{len(grist_team_id_to_name)} teams, max_id={grist_max_record_id}"
        )

        return True


async def notify_owner(context: ContextTypes.DEFAULT_TYPE, message: str) -> None:
    """Send error alerts to bot owner if OWNER_TELEGRAM_ID is configured."""
    if OWNER_TELEGRAM_ID is None:
        return

    try:
        await context.bot.send_message(chat_id=OWNER_TELEGRAM_ID, text=message)
    except Exception as e:
        logger.error(f"Failed to notify owner: {e}")


async def update_grist_people_matrix_id(handle: str, matrix_id: str) -> tuple[bool, str | None]:
    """Write Matrix ID to Grist People table for the user behind telegram handle."""
    normalized = normalize_telegram_handle(handle)
    person_row_id = grist_handle_to_person_row_id.get(normalized)
    if person_row_id is None:
        return False, "PERSON_ROW_ID_MISSING"

    url = f"https://grist.insomniafest.ru/api/docs/{GRIST_DOC_ID}/tables/{GRIST_PEOPLE_TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {GRIST_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "records": [
            {
                "id": person_row_id,
                "fields": {
                    "matrix_id": matrix_id,
                },
            }
        ]
    }

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "PATCH",
                url,
                headers=headers,
                json=payload,
            )
    except Exception as e:
        logger.warning(f"Failed to update People.matrix_id for {normalized}: {e}")
        return False, "PEOPLE_UPDATE_EXCEPTION"

    if response.status_code not in (200, 201):
        logger.warning(
            f"Failed to update People.matrix_id for {normalized}: {response.status_code} {response.text}"
        )
        return False, "PEOPLE_UPDATE_FAILED"

    return True, None


async def clear_fake_telegram_handle_in_grist(handle: str) -> tuple[bool, str | None]:
    """Remove fake Telegram handle from Participations rows after successful registration."""
    normalized = normalize_telegram_handle(handle)
    if not normalized:
        return False, "HANDLE_EMPTY"

    try:
        records = await fetch_grist_records_via_records_api()
    except Exception as e:
        logger.warning(f"Failed to fetch records for fake handle cleanup ({normalized}): {e}")
        return False, "CLEANUP_FETCH_FAILED"

    row_ids = []
    for record in records:
        fields = record.get("fields", {})
        telegram2 = normalize_telegram_handle(fields.get("Telegram2"))
        if telegram2 != normalized:
            continue

        record_id = fields.get("id")
        if record_id is None:
            record_id = record.get("id")
        record_id = parse_grist_ref_id(record_id)
        if record_id is not None:
            row_ids.append(record_id)

    if not row_ids:
        return True, None

    url = f"https://grist.insomniafest.ru/api/docs/{GRIST_DOC_ID}/tables/{GRIST_TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {GRIST_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "records": [
            {
                "id": row_id,
                "fields": {
                    "Telegram2": "",
                },
            }
            for row_id in row_ids
        ]
    }

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "PATCH",
                url,
                headers=headers,
                json=payload,
            )
    except Exception as e:
        logger.warning(f"Failed to cleanup fake Telegram handle {normalized}: {e}")
        return False, "CLEANUP_EXCEPTION"

    if response.status_code not in (200, 201):
        logger.warning(
            "Failed to cleanup fake Telegram handle %s: %s %s",
            normalized,
            response.status_code,
            response.text,
        )
        return False, "CLEANUP_FAILED"

    return True, None


async def check_synapse_admin_token() -> tuple[bool, str | None]:
    """Verify the Synapse admin token by calling a lightweight admin endpoint.

    Returns (True, None) on success, (False, error_message) on failure.
    """
    url = f"{SYNAPSE_API_URL}/_synapse/admin/v1/server_version"
    headers = {"Authorization": f"Bearer {SYNAPSE_ADMIN_TOKEN}"}
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await client.get(url, headers=headers)
        if response.status_code == 200:
            return True, None
        if response.status_code in (401, 403):
            return False, f"Admin token rejected by Synapse (HTTP {response.status_code})"
        return False, f"Unexpected Synapse response: HTTP {response.status_code}"
    except Exception as exc:
        return False, f"Could not reach Synapse: {format_exception_chain(exc)}"


async def get_synapse_registration_status(username: str) -> str:
    """Return registered/not_registered/unknown for a Matrix account localpart."""
    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        logger.warning("SYNAPSE_ADMIN_ACCESS_TOKEN is not set; cannot check registration status")
        return "unknown"

    user_id = quote(to_mxid(username), safe='')
    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "GET",
                f"{SYNAPSE_API_URL}/_synapse/admin/v2/users/{user_id}",
                headers=headers,
            )

        if response.status_code == 200:
            return "registered"
        if response.status_code == 404:
            return "not_registered"

        logger.warning(
            f"Failed to lookup registration status for {username}: {response.status_code} {response.text}"
        )
        return "unknown"
    except Exception as e:
        logger.warning(f"Error looking up registration status for {username}: {e}")
        return "unknown"

async def post_init(application: Application) -> None:
    """Notify owner that bot started and basic configuration is loaded."""
    if OWNER_TELEGRAM_ID is None:
        logger.warning("OWNER_TELEGRAM_ID is not set; owner notifications are disabled")

    token_ok, token_error = await check_synapse_admin_token()
    if not token_ok:
        logger.critical("Synapse admin token check failed: %s", token_error)
        if OWNER_TELEGRAM_ID is not None:
            try:
                await application.bot.send_message(
                    chat_id=OWNER_TELEGRAM_ID,
                    text=f"🚨 Бот запущен, но токен Synapse недействителен:\n{token_error}",
                )
            except Exception as e:
                logger.error("Failed to send token error notification to owner: %s", e)

    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        logger.warning("SYNAPSE_ADMIN_ACCESS_TOKEN is not set; auto-join via bot is disabled")

    sync_ok = await sync_grist_cache(force_full=True)
    if not sync_ok:
        logger.critical("Initial Grist cache sync failed, stopping bot startup")
        raise RuntimeError("Initial Grist cache sync failed")

    if OWNER_TELEGRAM_ID is None:
        return

    try:
        token_status = "✅ Токен Synapse действителен" if token_ok else f"❌ Токен Synapse: {token_error}"
        autojoin_status = (
            "✅ Авто-добавление в комнаты активно"
            if SYNAPSE_ADMIN_ACCESS_TOKEN
            else "⚠️ SYNAPSE_ADMIN_ACCESS_TOKEN не задан — авто-добавление отключено"
        )
        cache_status = (
            f"Кэш Grist: {len(grist_handle_to_record_id)} пользователей, max_id={grist_max_record_id}"
            if sync_ok
            else "Кэш Grist: не удалось обновить при старте"
        )
        await application.bot.send_message(
            chat_id=OWNER_TELEGRAM_ID,
            text=f"✅ Бот запущен. Уведомления об ошибках активны.\n{token_status}\n{autojoin_status}\n{cache_status}",
        )
    except Exception as e:
        logger.error(f"Failed to send startup notification to owner: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    message = """
Привет! Я бот для регистрации в чате для волонтеров. Чтобы зарегистрироваться, отправьте команду /register.

Важно: ваш аккаунт в нашем чате будет создан с тем же именем пользователя, что и в Telegram, чтобы ваши друзья и коллеги могли легко вас найти.
    """
    await update.message.reply_text(message)


async def register(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle registration request."""
    user_id = update.effective_user.id
    original_handle = normalize_telegram_handle(update.effective_user.username) or str(user_id)
    username = registration_localpart_from_handle(original_handle) or str(user_id)
    fake_tg_handle = is_fake_telegram_handle(original_handle)
    
    # Rate limiting check
    now = time.time()
    prune_registration_times(now)
    if user_id in user_registration_times:
        time_since_last_attempt = now - user_registration_times[user_id]
        if time_since_last_attempt < REGISTRATION_RATE_LIMIT:
            remaining_minutes = int((REGISTRATION_RATE_LIMIT - time_since_last_attempt) / 60) + 1
            await update.message.reply_text(
                f"⏳ Вы уже пробовали регистрироваться. Подождите {remaining_minutes} минут и попробуйте снова."
            )
            logger.warning(f"Rate limit exceeded for user {user_id} ({username})")
            return
    
    # Update registration attempt timestamp
    user_registration_times[user_id] = now
    
    try:
        await update.message.reply_text("Проверяю вашу благонадежность...")
        
        # Check if user is in the Grist list
        is_eligible, eligibility_check_ok, person_name, team_memberships = await check_user_eligibility(original_handle)

        if not eligibility_check_ok:
            await update.message.reply_text(
                "❌ Не удалось проверить данные регистрации. Пожалуйста, попробуйте еще раз через пару минут."
            )
            return
        
        if not is_eligible:
            await update.message.reply_text("""
❌ Ничего не вышло. Скорее всего ваш HR не добавил вас в список волонтеров 2026 (шепните ему волшебное слово: "Участия 2026"). Попросите его это сделать, а потом попробуйте снова.
            """)
            return
        
        # Register user in Synapse
        temp_password = secrets.token_urlsafe(12)
        is_organizer = any(team_memberships.values())
        success, registration_error_code = await register_synapse_user(
            username,
            temp_password,
        )
        account_reactivated = False

        if not success and registration_error_code == "M_USER_IN_USE":
            reactivation_ok, reactivation_error_code = await reactivate_synapse_user(
                username,
                temp_password,
            )
            if reactivation_ok:
                success = True
                registration_error_code = None
                account_reactivated = True
            elif reactivation_error_code not in ("ACCOUNT_ACTIVE", "REACTIVATION_TOKEN_MISSING"):
                await notify_owner(
                    context,
                    (
                        "⚠️ Не удалось реактивировать пользователя\n"
                        f"username={username}\n"
                        f"reactivation_error={reactivation_error_code}"
                    ),
                )
        
        if success:
            if person_name:
                displayname_ok = await set_synapse_display_name(username, person_name)
                if not displayname_ok:
                    logger.warning(f"Could not set display name for {username} to '{person_name}'")

            mxid = to_mxid(username)
            people_update_ok, people_update_error = await update_grist_people_matrix_id(original_handle, mxid)
            if not people_update_ok and people_update_error != "PERSON_ROW_ID_MISSING":
                await notify_owner(
                    context,
                    (
                        "⚠️ Не удалось обновить matrix_id в таблице People в Гристе\n"
                        f"username={username}\n"
                        f"matrix_id={mxid}\n"
                        f"people_update_error={people_update_error}"
                    ),
                )

            if fake_tg_handle:
                cleanup_ok, cleanup_error = await clear_fake_telegram_handle_in_grist(original_handle)
                if not cleanup_ok:
                    await notify_owner(
                        context,
                        (
                            "⚠️ Не удалось очистить фейковый Telegram handle в Гристе\n"
                            f"username={username}\n"
                            f"fake_handle={original_handle}\n"
                            f"cleanup_error={cleanup_error}"
                        ),
                    )

            room_aliases = list(AUTO_JOIN_ROOMS)
            if is_organizer:
                room_aliases.append(ORGS_ROOM)

            join_ok, failed_rooms = await join_user_to_rooms(username, room_aliases)
            team_join_ok, failed_team_rooms, failed_moderation_rooms = await join_user_to_team_rooms(
                username,
                team_memberships,
            )
            safe_username = html.escape(username)
            safe_password = html.escape(temp_password)
            safe_element_url = html.escape(ELEMENT_URL)
            safe_help_url = html.escape(HELP_URL)
            message = (
                "✅ Поздравляем!\n\n"
                "Вы можете войти в чат для волонтеров, используя следующие учетные данные:\n\n"
                "<b>Имя пользователя:</b>\n"
                f"<code>{safe_username}</code>\n\n"
                "<b>Временный пароль:</b>\n"
                f"<code>{safe_password}</code>\n\n"
                "Обязательно поменяйте пароль при первом входе!\n\nВ мобильном приложении этого сделать нельзя, поэтому "
                f"используйте <a href=\"{safe_element_url}\">браузерную версию</a> "
                "или десктоп-клиент.\n\n"
                f"🔗 <b>Ссылка на чат:</b> <a href=\"{safe_element_url}\">{safe_element_url}</a>\n"
                f"📖 <b>Вкратце о Чате:</b> <a href=\"{safe_help_url}\">{safe_help_url}</a>"
            )
            if account_reactivated:
                message += "\n♻️ Ваш аккаунт был восстановлен после деактивации."
            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

            if not join_ok and failed_rooms:
                await update.message.reply_text(
                    "⚠️ Аккаунт создан, но не удалось автоматически добавить вас в комнаты: "
                    f"{', '.join(failed_rooms)}.\n"
                    "Попросите любого другого пользователя отправить вам инвайт в эти комнаты."
                )

                await notify_owner(
                    context,
                    (
                        "⚠️ Автодобавление в комнаты не удалось\n"
                        f"username={username}\n"
                        f"failed_rooms={', '.join(failed_rooms)}"
                    ),
                )

            if not team_join_ok and failed_team_rooms:
                await update.message.reply_text(
                    "⚠️ Аккаунт создан, но не удалось автоматически добавить вас в командные комнаты: "
                    f"{', '.join(failed_team_rooms)}."
                )

                await notify_owner(
                    context,
                    (
                        "⚠️ Автодобавление в командные комнаты не удалось\n"
                        f"username={username}\n"
                        f"failed_team_rooms={', '.join(failed_team_rooms)}"
                    ),
                )

            if failed_moderation_rooms:
                await update.message.reply_text(
                    "⚠️ Вы добавлены в командные комнаты, но не удалось выдать права администратора в: "
                    f"{', '.join(failed_moderation_rooms)}."
                )

                await notify_owner(
                    context,
                    (
                        "⚠️ Не удалось выдать права администратора в командных комнатах\n"
                        f"username={username}\n"
                        f"failed_moderation_rooms={', '.join(failed_moderation_rooms)}"
                    ),
                )
        elif registration_error_code == "M_USER_IN_USE":
            await update.message.reply_text(
                f"""⚠️ Учетная запись "{username}" уже существует.

🔗 Попробуйте войти тут: {ELEMENT_URL}
📖 Вкратце о Бессонном Чате: {HELP_URL}

Вы можете сбросить пароль, отправив команду /reset_password."""
            )
        else:
            await notify_owner(
                context,
                (
                    "⚠️ Не удалось зарегистрировать пользователя\n"
                    f"username={username}\n"
                    f"registration_error={registration_error_code}"
                ),
            )
            await update.message.reply_text("❌ Не удалось создать учетную запись. Администраторы получили сообщение об этом и постараются как можно скорее всё починить.")
            
    except Exception as e:
        logger.error(f"Registration error for user {user_id}: {e}")
        await notify_owner(
            context,
            f"⚠️ Ошибка регистрации\nuser_id={user_id}\nusername={username}\nerror={e}",
        )
        await update.message.reply_text("❌ Произошла ошибка при регистрации. Администраторы получили сообщение об этом и постараются как можно скорее всё починить")


async def reset_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle password reset request for existing Matrix accounts."""
    user_id = update.effective_user.id
    username = normalize_telegram_handle(update.effective_user.username) or str(user_id)

    now = time.time()
    prune_registration_times(now)
    if user_id in user_registration_times:
        time_since_last_attempt = now - user_registration_times[user_id]
        if time_since_last_attempt < REGISTRATION_RATE_LIMIT:
            remaining_minutes = int((REGISTRATION_RATE_LIMIT - time_since_last_attempt) / 60) + 1
            await update.message.reply_text(
                f"⏳ Вы уже пробовали сбросить пароль. Подождите {remaining_minutes} минут и попробуйте снова."
            )
            logger.warning(f"Password reset rate limit exceeded for password reset {user_id} ({username})")
            return

    user_registration_times[user_id] = now

    try:
        await update.message.reply_text("Проверяю вашу благонадежность...")

        is_eligible, eligibility_check_ok, _, _ = await check_user_eligibility(username)

        if not eligibility_check_ok:
            await update.message.reply_text(
                "❌ Не удалось проверить данные. Пожалуйста, попробуйте еще раз через пару минут."
            )
            return

        if not is_eligible:
            await update.message.reply_text(
                "❌ Ваш Telegram-аккаунт не найден в списке волонтеров 2026."
            )
            return

        temp_password = secrets.token_urlsafe(12)
        reset_ok, reset_error = await reset_synapse_password(username, temp_password)

        if not reset_ok:
            if reset_error == "RESET_TOKEN_MISSING":
                await update.message.reply_text(
                    "❌ Сброс пароля временно недоступен. Обратитесь к администратору."
                )
            else:
                await update.message.reply_text(
                    "❌ Не удалось сбросить пароль. Попробуйте сначала зарегистрироваться: /register."
                )
                await notify_owner(
                    context,
                    (
                        "⚠️ Не удалось выполнить сброс пароля\n"
                        f"username={username}\n"
                        f"reset_error={reset_error}"
                    ),
                )
            return

        safe_username = html.escape(username)
        safe_password = html.escape(temp_password)
        safe_element_url = html.escape(ELEMENT_URL)
        safe_help_url = html.escape(HELP_URL)
        await update.message.reply_text(
            (
                "✅ Пароль сброшен!\n\n"
                "<b>Имя пользователя:</b>\n"
                f"<code>{safe_username}</code>\n\n"
                "<b>Временный пароль:</b>\n"
                f"<code>{safe_password}</code>\n\n"
                "Поменяйте пароль сразу после входа.\n\n"
                f"🔗 <b>Ссылка на чат:</b> <a href=\"{safe_element_url}\">{safe_element_url}</a>\n"
                f"📖 <b>Вкратце о Чате:</b> <a href=\"{safe_help_url}\">{safe_help_url}</a>"
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error(f"Password reset error for user {user_id}: {e}")
        await notify_owner(
            context,
            f"⚠️ Ошибка сброса пароля\nuser_id={user_id}\nusername={username}\nerror={e}",
        )
        await update.message.reply_text("❌ Произошла ошибка при сбросе пароля. Администраторы получили сообщение об этом и постараются как можно скорее всё починить")


def format_exception_chain(error: BaseException, max_depth: int = 4) -> str:
    """Build compact exception chain string for logs."""
    parts = []
    current = error
    depth = 0

    while current is not None and depth < max_depth:
        parts.append(f"{current.__class__.__name__}: {current}")
        current = current.__cause__
        depth += 1

    if current is not None:
        parts.append("...")

    return " <- ".join(parts)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle unexpected telegram framework errors and alert owner."""
    if isinstance(context.error, NetworkError):
        details = format_exception_chain(context.error)
        logger.warning("Transient network error (will retry): %s", details)
        return

    logger.error("Unhandled exception in Telegram handler", exc_info=context.error)

    user_id = "unknown"
    username = "unknown"
    if isinstance(update, Update) and update.effective_user:
        user_id = str(update.effective_user.id)
        username = update.effective_user.username or "no_username"

    await notify_owner(
        context,
        (
            "⚠️ Необработанная ошибка бота\n"
            f"user_id={user_id}\n"
            f"username={username}\n"
            f"error={context.error}"
        ),
    )

    if isinstance(update, Update) and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Произошла внутренняя ошибка. Пожалуйста, попробуйте снова чуть позже.",
            )
        except Exception as e:
            logger.error(f"Failed to send fallback error message to user: {e}")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send help information."""
    message = (
        "📖 Помощь и документация\n\n"
        f"Вкратце о Бессонном Чате: {HELP_URL}\n\n"
        "/register - создать аккаунт в Matrix.\n"
        "/my_teams - добавиться в командные комнаты.\n"
        "/reset_password - сбросить пароль существующего аккаунта.\n\n"
        "Если возникнут вопросы или проблемы, обратитесь к своему HR или напишите в Общий Чат https://chat.insomniafest.ru/#/room/#general:insomniafest.ru."
    )

    if await is_hr_command_user(update):
        message += (
            "\n\n"
            "🔐 Команды HR\n\n"
            "(Все данные бот берет из таблицы Участия 2026 в Гристе)\n\n"
            "Как это работает:\n"
            "- /hr_check и /hr_sync_teams принимают @телеграм_ник или Matrix ID вида @username:insomniafest.ru.\n"
            "- Если в Гристе для регистрации используется временный telegram-ник с ведущими '_' (например, __ivan), "
            "то при /hr_register эти '_' убираются из Matrix-логина.\n"
            "- После успешной регистрации временный telegram-ник очищается в Гристе автоматически.\n\n"
            "/hr_sync - принудительно обновить кэш Грист и показать счетчики (полезно сделать если человек был только что добавлен в Участия 2026).\n"
            "/hr_check @телеграм_ник | @username:insomniafest.ru - проверить есть ли человек в Участиях 2026 и членство в командах.\n"
            "/hr_register @телеграм_ник - выполнить полную регистрацию: аккаунт в чате и автодобавление в комнаты.\n"
            "/hr_sync_teams @телеграм_ник | @username:insomniafest.ru - перепроверить команды участника и добавить в командные комнаты (создаст комнаты при необходимости)."
        )

    await update.message.reply_text(message)


async def prepare_team_sync_target(handle: str) -> tuple[str, str | None, dict[int, bool], str | None]:
    """Resolve eligibility and registration for team sync.

    Returns (normalized_handle, person_name, memberships, error_code).
    """
    eligible, check_ok, person_name, memberships = await check_user_eligibility(handle)

    if not check_ok:
        return "", None, {}, "CHECK_FAILED"

    if not eligible:
        return "", None, {}, "NOT_ELIGIBLE"

    normalized_handle = (
        matrix_localpart_from_id(handle)
        if is_matrix_id(handle)
        else normalize_telegram_handle(handle)
    )
    registration_status = await get_synapse_registration_status(normalized_handle)
    if registration_status != "registered":
        return normalized_handle, person_name, memberships, f"NOT_REGISTERED:{registration_status}"

    if not memberships:
        return normalized_handle, person_name, memberships, "NO_MEMBERSHIPS"

    return normalized_handle, person_name, memberships, None


def split_team_sync_results(team_results: list[dict[str, object]]) -> tuple[list[str], list[str], list[str]]:
    """Split per-team sync results into already joined, newly joined, failed lists."""
    already_joined = []
    newly_joined = []
    failed_team_rooms = []

    for result in team_results:
        team_name = html.escape(str(result["team_name"]))
        role = "организатор" if result["is_organizer"] else "участник"
        room_id = result.get("room_id")
        room_link = build_matrix_room_link(str(room_id)) if room_id else None
        line = (
            f"- <a href=\"{html.escape(room_link)}\">{team_name}</a> ({role})"
            if room_link
            else f"- {team_name} ({role})"
        )

        if result["status"] == "already_joined":
            already_joined.append(line)
        elif result["status"] == "joined":
            newly_joined.append(line)
        else:
            failed_team_rooms.append(team_name)

    return already_joined, newly_joined, failed_team_rooms


def build_team_sync_message(
    team_results: list[dict[str, object]],
    failed_moderation_rooms: list[str],
    title: str,
    already_title: str,
    newly_title: str,
    header_lines: list[str] | None = None,
) -> str:
    """Build rich HTML message for team sync results."""
    already_joined, newly_joined, failed_team_rooms = split_team_sync_results(team_results)

    message_parts = [title]
    if header_lines:
        message_parts.append("\n\n" + "\n".join(header_lines))

    if already_joined:
        message_parts.append("\n\n" + already_title + "\n\n" + "\n".join(already_joined))

    if newly_joined:
        message_parts.append("\n\n" + newly_title + "\n\n" + "\n".join(newly_joined))

    if failed_team_rooms:
        message_parts.append(
            "\n\n⚠️ Не удалось добавить в комнаты: " + ", ".join(failed_team_rooms) + "."
        )

    if failed_moderation_rooms:
        message_parts.append(
            "\n⚠️ Не удалось выдать права администратора в комнатах: "
            + ", ".join(failed_moderation_rooms)
            + "."
        )

    return "".join(message_parts)


async def my_teams(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check caller participation and ensure membership in all team chats."""
    user_id = update.effective_user.id
    username = normalize_telegram_handle(update.effective_user.username) or str(user_id)

    await update.message.reply_text("Проверяю ваши команды и доступ в командные комнаты...")

    normalized_handle, _, memberships, error_code = await prepare_team_sync_target(username)

    if error_code == "CHECK_FAILED":
        await update.message.reply_text(
            "❌ Не удалось проверить данные регистрации. Попробуйте снова через пару минут."
        )
        return

    if error_code == "NOT_ELIGIBLE":
        await update.message.reply_text(
            "❌ Ваш Telegram-аккаунт не найден в Участиях 2026."
        )
        return

    if error_code and error_code.startswith("NOT_REGISTERED:"):
        await update.message.reply_text(
            "ℹ️ Ваш аккаунт в чате пока не зарегистрирован. Сначала используйте /register, "
            "после этого можно снова вызвать /my_teams."
        )
        return

    if error_code == "NO_MEMBERSHIPS":
        await update.message.reply_text(
            "ℹ️ Команды в Участиях 2026 для вас не найдены."
        )
        return

    team_results, failed_moderation_rooms = await sync_user_to_team_rooms_detailed(
        normalized_handle,
        memberships,
    )
    message = build_team_sync_message(
        team_results,
        failed_moderation_rooms,
        title="✅ Проверил статус вашего участия в командах.",
        already_title="Вы уже были в этих командных комнатах:",
        newly_title="Вы были добавлены в эти комнаты:",
    )

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


def is_admin_telegram_user(update: Update) -> bool:
    """Check whether Telegram user is allowed by static admin list."""
    if not update or not update.effective_user:
        return False
    return update.effective_user.id in ADMIN_TELEGRAM_IDS


async def is_hr_command_user(update: Update) -> bool:
    """Check whether Telegram user can run hidden HR commands."""
    if is_admin_telegram_user(update):
        return True

    if not update or not update.effective_user:
        return False

    handle = normalize_telegram_handle(update.effective_user.username)
    if not handle:
        return False

    if grist_handle_to_is_hr_now.get(handle, False):
        return True

    if any(grist_handle_to_team_memberships.get(handle, {}).values()):
        return True

    sync_ok = await sync_grist_cache(force_full=False)
    if not sync_ok:
        return False

    return grist_handle_to_is_hr_now.get(handle, False) or any(
        grist_handle_to_team_memberships.get(handle, {}).values()
    )


async def require_hr(update: Update) -> bool:
    """Return True for static-admin/HR users, otherwise send denial message."""
    if await is_hr_command_user(update):
        return True

    await update.message.reply_text("❌ Эта команда недоступна.")
    return False


async def require_admin(update: Update) -> bool:
    """Backward-compatible alias for hidden command permission check."""
    return await require_hr(update)


async def ops_sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Force Grist cache sync and report counters. Hidden admin-only command."""
    if not await require_hr(update):
        return

    ok = await sync_grist_cache(force_full=True)
    if not ok:
        await update.message.reply_text("❌ Не удалось обновить кэш Гриста.")
        return

    await update.message.reply_text(
        (
            "✅ Кэш Гриста обновлен\n"
            f"пользователей={len(grist_handle_to_record_id)}\n"
            f"команд={len(grist_team_id_to_name)}\n"
            f"макс_id_записи={grist_max_record_id}"
        )
    )


async def ops_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check eligibility and team memberships for a Telegram handle. Hidden admin-only command."""
    if not await require_hr(update):
        return

    if not context.args:
        await update.message.reply_text("Использование: /hr_check <телеграм_ник>")
        return

    handle = context.args[0]
    eligible, check_ok, person_name, memberships = await check_user_eligibility(handle)

    if not check_ok:
        await update.message.reply_text("❌ Не удалось проверить данные участника")
        return

    if not eligible:
        await update.message.reply_text(f"❌ Участник не найден: {handle}")
        return

    is_matrix_lookup = is_matrix_id(handle)
    normalized_handle = matrix_localpart_from_id(handle) if is_matrix_lookup else normalize_telegram_handle(handle)
    registration_status = await get_synapse_registration_status(normalized_handle)
    registration_status_ru = {
        "registered": "уже зарегистрирован",
        "not_registered": "не зарегистрирован",
        "unknown": "не удалось определить",
    }.get(registration_status, "не удалось определить")

    lines = [
        "✅ Участник найден в списке Участий 2026",
        (
            f"Matrix ID: {normalize_matrix_id(handle)}"
            if is_matrix_lookup
            else f"Telegram: @{normalized_handle}"
        ),
        f"Имя: {person_name or '-'}",
        f"Регистрация в Matrix: {registration_status_ru}",
    ]
    if memberships:
        for team_id, is_org in sorted(memberships.items()):
            lines.append(
                f"Команда #{team_id}: {get_team_name(team_id)}; роль: {'организатор' if is_org else 'участник'}"
            )
    else:
        lines.append("Команды: не указаны")

    await update.message.reply_text("\n".join(lines))


async def ops_register(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run full registration flow for a provided handle. Hidden admin-only command."""
    if not await require_hr(update):
        return

    if not context.args:
        await update.message.reply_text("Использование: /hr_register <телеграм_ник>")
        return

    handle = context.args[0]
    eligible, check_ok, person_name, memberships = await check_user_eligibility(handle)

    if not check_ok:
        await update.message.reply_text("❌ Не удалось проверить данные участника")
        return

    if not eligible:
        await update.message.reply_text(f"❌ Участник не найден: {handle}")
        return

    original_handle = normalize_telegram_handle(handle)
    username = registration_localpart_from_handle(original_handle)
    fake_tg_handle = is_fake_telegram_handle(original_handle)
    temp_password = secrets.token_urlsafe(12)
    is_organizer = any(memberships.values())

    register_ok, registration_error = await register_synapse_user(username, temp_password)
    reactivated = False

    if not register_ok and registration_error == "M_USER_IN_USE":
        reactivation_ok, reactivation_error = await reactivate_synapse_user(username, temp_password)
        if reactivation_ok:
            register_ok = True
            registration_error = None
            reactivated = True
        elif reactivation_error not in ("ACCOUNT_ACTIVE", "REACTIVATION_TOKEN_MISSING"):
            await update.message.reply_text(
                f"❌ Не удалось реактивировать аккаунт {username}: {reactivation_error}"
            )
            return

    created = register_ok and not reactivated
    if not register_ok and registration_error != "M_USER_IN_USE":
        await update.message.reply_text(
            f"❌ Не удалось зарегистрировать {username}: {registration_error}"
        )
        return

    displayname_ok = True
    if person_name:
        displayname_ok = await set_synapse_display_name(username, person_name)

    if fake_tg_handle:
        cleanup_ok, cleanup_error = await clear_fake_telegram_handle_in_grist(original_handle)
        if not cleanup_ok:
            lines = [
                "⚠️ Регистрация завершена, но не удалось очистить фейковый Telegram в Гристе.",
                f"ошибка={cleanup_error}",
            ]
            await update.message.reply_text("\n".join(lines))

    room_aliases = list(AUTO_JOIN_ROOMS)
    if is_organizer:
        room_aliases.append(ORGS_ROOM)

    join_ok, failed_rooms = await join_user_to_rooms(username, room_aliases)
    team_join_ok, failed_team_rooms, failed_moderation_rooms = await join_user_to_team_rooms(
        username,
        memberships,
    )

    lines = [
        "🧪 Полная регистрация через HR",
        f"пользователь={username}",
        f"mxid={to_mxid(username)}",
        f"имя={person_name or '-'}",
        f"создан={str(created).lower()}",
        f"реактивирован={str(reactivated).lower()}",
        f"отображаемое_имя_обновлено={str(displayname_ok).lower()}",
        f"добавление_в_базовые_комнаты={str(join_ok).lower()}",
        f"добавление_в_командные_комнаты={str(team_join_ok).lower()}",
    ]

    if created:
        lines.append(f"временный_пароль={temp_password}")

    if failed_rooms:
        lines.append(f"не_добавлен_в_комнаты={', '.join(failed_rooms)}")

    if failed_team_rooms:
        lines.append(f"не_добавлен_в_командные_комнаты={', '.join(failed_team_rooms)}")

    if failed_moderation_rooms:
        lines.append(f"не_выданы_права_администратора={', '.join(failed_moderation_rooms)}")

    await update.message.reply_text("\n".join(lines))


async def ops_sync_teams(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sync team room membership for a provided Telegram handle. Hidden HR-only command."""
    if not await require_hr(update):
        return

    if not context.args:
        await update.message.reply_text("Использование: /hr_sync_teams <телеграм_ник>")
        return

    handle = context.args[0]
    normalized_handle, person_name, memberships, error_code = await prepare_team_sync_target(handle)

    if error_code == "CHECK_FAILED":
        await update.message.reply_text("❌ Не удалось проверить данные участника")
        return

    if error_code == "NOT_ELIGIBLE":
        await update.message.reply_text(f"❌ Участник не найден: {handle}")
        return

    if error_code and error_code.startswith("NOT_REGISTERED:"):
        registration_status = error_code.split(":", 1)[1]
        await update.message.reply_text(
            f"❌ {normalized_handle} пока не зарегистрирован в Matrix ({registration_status})."
        )
        return

    if error_code == "NO_MEMBERSHIPS":
        await update.message.reply_text(
            f"ℹ️ Для @{normalized_handle} не найдено команд в Участиях 2026."
        )
        return

    team_results, failed_moderation_rooms = await sync_user_to_team_rooms_detailed(
        normalized_handle,
        memberships,
    )

    safe_handle = html.escape(normalized_handle)
    safe_name = html.escape(person_name or "-")
    message = build_team_sync_message(
        team_results,
        failed_moderation_rooms,
        title="✅ Проверил статус участия в командах.",
        already_title="Уже был в этих командных комнатах:",
        newly_title="Был добавлен в эти комнаты:",
        header_lines=[
            f"Пользователь: @{safe_handle}",
            f"Имя: {safe_name}",
        ],
    )

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


async def check_user_eligibility(telegram_handle: str) -> tuple[bool, bool, str | None, dict[int, bool]]:
    """Return (is_eligible, check_ok, person_name, team_memberships) using in-memory Grist cache."""
    try:
        if not telegram_handle:
            logger.warning("Empty telegram handle provided")
            return False, True, None, {}

        if is_matrix_id(telegram_handle):
            matrix_id = normalize_matrix_id(telegram_handle)
            if not matrix_id:
                logger.warning("Invalid Matrix ID provided: %s", telegram_handle)
                return False, True, None, {}

            sync_ok = await sync_grist_cache(force_full=False)
            if not sync_ok and not grist_person_row_to_team_memberships:
                logger.warning("Grist cache unavailable and empty for matrix lookup")
                return False, False, None, {}

            people_sync_ok = await sync_grist_people_matrix_cache(force_full=False)
            if not people_sync_ok and not grist_matrix_id_to_person_row_id:
                logger.warning("Grist people cache unavailable and empty")
                return False, False, None, {}

            person_row_id = grist_matrix_id_to_person_row_id.get(matrix_id)
            if person_row_id is None:
                logger.warning(f"Matrix ID {matrix_id} not found in Grist people cache")
                return False, True, None, {}

            memberships = grist_person_row_to_team_memberships.get(person_row_id, {})
            person_name = grist_person_row_to_person_name.get(person_row_id)
            return True, True, person_name, dict(memberships)

        handle = normalize_telegram_handle(telegram_handle)

        # Fast path: do not call Grist if the handle is already in cache.
        if handle in grist_handle_to_record_id:
            logger.info(
                f"User {handle} found in Grist cache (record_id={grist_handle_to_record_id[handle]})"
            )
            memberships = grist_handle_to_team_memberships.get(handle, {})
            return True, True, grist_handle_to_person_name.get(handle), dict(memberships)

        # Cache miss: try syncing once, then re-check.
        sync_ok = await sync_grist_cache(force_full=False)

        # If Grist is temporarily unavailable, keep serving from stale cache.
        if not sync_ok and not grist_handle_to_record_id:
            logger.warning("Grist cache unavailable and empty")
            return False, False, None, {}

        if handle in grist_handle_to_record_id:
            logger.info(
                f"User {handle} found in Grist cache after sync (record_id={grist_handle_to_record_id[handle]})"
            )
            memberships = grist_handle_to_team_memberships.get(handle, {})
            return True, True, grist_handle_to_person_name.get(handle), dict(memberships)

        logger.warning(f"User {handle} not found in Grist cache")
        return False, True, None, {}
            
    except Exception as e:
        logger.error(f"Error checking eligibility for {telegram_handle}: {e}")
        return False, False, None, {}


async def register_synapse_user(username: str, password: str) -> tuple[bool, str | None]:
    """Register a user in Synapse using the shared secret method."""
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            for attempt in range(HTTP_RETRIES + 1):
                nonce_response = await request_with_retries(
                    client,
                    "GET",
                    f"{SYNAPSE_API_URL}/_synapse/admin/v1/register",
                )
                if nonce_response.status_code != 200:
                    logger.error(
                        f"Failed nonce request for {username}: {nonce_response.status_code} {nonce_response.text}"
                    )
                    return False, "NONCE_REQUEST_FAILED"
                nonce_data = nonce_response.json()
                nonce = nonce_data.get('nonce')

                if not nonce:
                    logger.error(f"Failed to obtain nonce for {username}")
                    return False, "NONCE_MISSING"

                admin_flag = "notadmin"
                msg = "\x00".join([nonce, username, password, admin_flag]).encode("utf-8")
                secret_bytes = SYNAPSE_ADMIN_TOKEN.encode("utf-8")
                mac = hmac.new(secret_bytes, msg, hashlib.sha1).hexdigest()

                payload = {
                    "nonce": nonce,
                    "username": username,
                    "password": password,
                    "admin": False,
                    "mac": mac,
                }

                try:
                    register_response = await client.post(
                        f"{SYNAPSE_API_URL}/_synapse/admin/v1/register",
                        json=payload,
                    )
                except httpx.RequestError as e:
                    if attempt == HTTP_RETRIES:
                        raise
                    backoff_seconds = 0.5 * (2 ** attempt)
                    logger.warning(
                        "Registration POST failed for %s; refreshing nonce and retrying %d/%d: %s",
                        username,
                        attempt + 1,
                        HTTP_RETRIES,
                        format_exception_chain(e),
                    )
                    await asyncio.sleep(backoff_seconds)
                    continue

                if register_response.status_code == 200:
                    logger.info(f"User {username} registered successfully")
                    return True, None

                error_code = None
                try:
                    error_code = register_response.json().get("errcode")
                except Exception:
                    error_code = None

                error_text = (register_response.text or "").lower()
                if register_response.status_code == 400 and error_code == "M_USER_IN_USE":
                    logger.warning(f"User {username} already exists in Synapse")
                    return False, "M_USER_IN_USE"

                if register_response.status_code == 400 and "unrecognised nonce" in error_text:
                    if attempt == HTTP_RETRIES:
                        logger.error(
                            f"Failed to register user {username}: {register_response.status_code} {register_response.text}"
                        )
                        return False, "REGISTER_FAILED"
                    backoff_seconds = 0.5 * (2 ** attempt)
                    logger.warning(
                        "Synapse rejected stale nonce for %s; retrying full registration handshake %d/%d",
                        username,
                        attempt + 1,
                        HTTP_RETRIES,
                    )
                    await asyncio.sleep(backoff_seconds)
                    continue

                logger.error(
                    f"Failed to register user {username}: {register_response.status_code} {register_response.text}"
                )
                return False, error_code or "REGISTER_FAILED"
                
    except Exception as e:
        logger.error(f"Error registering user {username}: {e}")
        return False, "REGISTER_EXCEPTION"


async def reactivate_synapse_user(username: str, password: str) -> tuple[bool, str | None]:
    """Reactivate deactivated user and set a new password via Synapse Admin API."""
    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        logger.warning("SYNAPSE_ADMIN_ACCESS_TOKEN is not set; cannot reactivate users")
        return False, "REACTIVATION_TOKEN_MISSING"

    user_id = quote(to_mxid(username), safe='')
    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            get_response = await request_with_retries(
                client,
                "GET",
                f"{SYNAPSE_API_URL}/_synapse/admin/v2/users/{user_id}",
                headers=headers,
            )
            if get_response.status_code != 200:
                logger.warning(
                    f"Failed to lookup user {username}: {get_response.status_code} {get_response.text}"
                )
                return False, "USER_LOOKUP_FAILED"

            try:
                user_data = get_response.json()
            except Exception:
                user_data = {}

            if not bool(user_data.get("deactivated")):
                return False, "ACCOUNT_ACTIVE"

            put_response = await request_with_retries(
                client,
                "PUT",
                f"{SYNAPSE_API_URL}/_synapse/admin/v2/users/{user_id}",
                headers=headers,
                json={"deactivated": False, "password": password},
            )

        if put_response.status_code not in (200, 201):
            logger.warning(
                f"Failed to reactivate user {username}: {put_response.status_code} {put_response.text}"
            )
            return False, "REACTIVATION_FAILED"

        logger.info(f"User {username} reactivated successfully")
        return True, None
    except Exception as e:
        logger.warning(f"Error reactivating user {username}: {e}")
        return False, "REACTIVATION_EXCEPTION"


async def reset_synapse_password(username: str, password: str) -> tuple[bool, str | None]:
    """Reset user password via Synapse Admin API."""
    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        logger.warning("SYNAPSE_ADMIN_ACCESS_TOKEN is not set; cannot reset passwords")
        return False, "RESET_TOKEN_MISSING"

    user_id = quote(to_mxid(username), safe='')
    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "PUT",
                f"{SYNAPSE_API_URL}/_synapse/admin/v2/users/{user_id}",
                headers=headers,
                json={"password": password},
            )

        if response.status_code not in (200, 201):
            logger.warning(
                f"Failed to reset password for {username}: {response.status_code} {response.text}"
            )
            return False, "RESET_FAILED"

        logger.info(f"Password reset for {username} completed")
        return True, None
    except Exception as e:
        logger.warning(f"Error resetting password for {username}: {e}")
        return False, "RESET_EXCEPTION"


async def set_synapse_display_name(username: str, display_name: str) -> bool:
    """Set display name for a user via Synapse Admin API."""
    if not display_name:
        return True

    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        logger.warning("SYNAPSE_ADMIN_ACCESS_TOKEN is not set; skipping display name update")
        return False

    user_id = quote(to_mxid(username), safe='')
    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "displayname": display_name,
    }

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "PUT",
                f"{SYNAPSE_API_URL}/_synapse/admin/v2/users/{user_id}",
                headers=headers,
                json=payload,
            )

        if response.status_code not in (200, 201):
            logger.warning(
                f"Failed to set display name for {username}: {response.status_code} {response.text}"
            )
            return False

        return True
    except Exception as e:
        logger.warning(f"Error while setting display name for {username}: {e}")
        return False


def to_mxid(localpart: str) -> str:
    """Build full MXID from a localpart."""
    return f"@{localpart}:{SYNAPSE_SERVER_NAME}"


def get_team_name(team_id: int) -> str:
    """Get team name by id, with a fallback."""
    team_name = grist_team_id_to_name.get(team_id)
    if isinstance(team_name, str) and team_name.strip():
        return team_name.strip()
    return f"Команда {team_id}"


def build_matrix_room_link(room_id: str) -> str:
    """Build a web link to a Matrix room in Element."""
    return f"{ELEMENT_URL}/#/room/{quote(room_id, safe='')}"


def build_team_room_alias(team_id: int) -> str:
    """Build deterministic alias for a team room."""
    return f"#team-{team_id}:{SYNAPSE_SERVER_NAME}"


async def resolve_room_alias(alias: str) -> str | None:
    """Resolve Matrix room alias to room id."""
    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        return None

    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
    }
    encoded_alias = quote(alias, safe='')

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "GET",
                f"{SYNAPSE_API_URL}/_matrix/client/v3/directory/room/{encoded_alias}",
                headers=headers,
            )

        if response.status_code != 200:
            return None

        return response.json().get("room_id")
    except Exception as e:
        logger.warning(f"Could not resolve alias {alias}: {e}")
        return None


async def create_team_room(team_id: int, team_name: str) -> str | None:
    """Create a private team room and return room id."""
    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        return None

    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    alias_localpart = f"team-{team_id}"
    payload = {
        "preset": "private_chat",
        "name": team_name,
        "topic": f"Команда: {team_name}",
        "room_alias_name": alias_localpart,
    }

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "POST",
                f"{SYNAPSE_API_URL}/_matrix/client/v3/createRoom",
                headers=headers,
                json=payload,
            )

            if response.status_code == 200:
                return response.json().get("room_id")

        # Race-safe fallback: room may have been created by another request with the same alias.
        alias = build_team_room_alias(team_id)
        room_id = await resolve_room_alias(alias)
        if room_id:
            return room_id

        logger.warning(
            f"Could not create room for team '{team_name}': "
            f"{response.status_code} {response.text}"
        )
        return None
    except Exception as e:
        logger.warning(f"Could not create room for team '{team_name}': {e}")
        return None


async def ensure_team_room(team_id: int, team_name: str) -> str | None:
    """Ensure team room exists and return room id."""
    alias = build_team_room_alias(team_id)
    room_id = await resolve_room_alias(alias)
    if room_id:
        return room_id

    return await create_team_room(team_id, team_name)


async def set_room_moderator(room_id: str, user_id: str) -> bool:
    """Set room power level to admin for selected user via Matrix Client API."""
    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        return False

    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    encoded_room_id = quote(room_id, safe='')
    power_levels_url = f"{SYNAPSE_API_URL}/_matrix/client/v3/rooms/{encoded_room_id}/state/m.room.power_levels"

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            get_response = await request_with_retries(
                client,
                "GET",
                power_levels_url,
                headers=headers,
            )

            payload = get_response.json() if get_response.status_code == 200 else {}
            users = payload.get("users")
            if not isinstance(users, dict):
                users = {}
                payload["users"] = users

            current_level = users.get(user_id, 0)
            if isinstance(current_level, int) and current_level >= TEAM_ROOM_ORGANIZER_POWER_LEVEL:
                return True

            users[user_id] = TEAM_ROOM_ORGANIZER_POWER_LEVEL
            put_response = await request_with_retries(
                client,
                "PUT",
                power_levels_url,
                headers=headers,
                json=payload,
            )

        if put_response.status_code not in (200, 201):
            logger.warning(
                f"Could not set admin for {user_id} in {room_id}: "
                f"{put_response.status_code} {put_response.text}"
            )
            return False

        return True
    except Exception as e:
        logger.warning(f"Could not set admin for {user_id} in {room_id}: {e}")
        return False


async def get_room_parent_spaces(room_id: str) -> list[str]:
    """Return parent space room IDs linked from a room via m.space.parent state."""
    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        return []

    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
    }
    encoded_room_id = quote(room_id, safe='')

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "GET",
                f"{SYNAPSE_API_URL}/_matrix/client/v3/rooms/{encoded_room_id}/state",
                headers=headers,
            )

        if response.status_code != 200:
            return []

        parent_spaces = []
        seen = set()
        for event in response.json():
            if not isinstance(event, dict):
                continue
            if event.get("type") != "m.space.parent":
                continue
            state_key = event.get("state_key")
            if not isinstance(state_key, str) or not state_key.startswith("!"):
                continue
            if state_key in seen:
                continue
            seen.add(state_key)
            parent_spaces.append(state_key)

        return parent_spaces
    except Exception as e:
        logger.warning(f"Could not fetch parent spaces for {room_id}: {e}")
        return []


async def join_user_to_room(username: str, room_alias_or_id: str) -> str:
    """Join a local user to a room and return joined/already_joined/failed."""
    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        logger.info("SYNAPSE_ADMIN_ACCESS_TOKEN is not set; auto-join via bot skipped (Synapse may handle it)")
        return "joined"

    user_id = to_mxid(username)
    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    room_id_or_alias = quote(room_alias_or_id, safe='')

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await request_with_retries(
                client,
                "POST",
                f"{SYNAPSE_API_URL}/_synapse/admin/v1/join/{room_id_or_alias}",
                headers=headers,
                json={"user_id": user_id},
            )
    except Exception as e:
        logger.error(f"Auto-join request failed for {user_id} to {room_alias_or_id}: {e}")
        return "failed"

    if response.status_code in (200, 201):
        return "joined"

    if response.status_code == 403:
        try:
            error_payload = response.json()
        except Exception:
            error_payload = {}

        error_text = str(error_payload.get("error") or response.text or "")
        if "already in the room" in error_text.lower():
            logger.info(f"{user_id} is already in {room_alias_or_id}; treating join as successful")
            return "already_joined"

    logger.warning(
        f"Failed to auto-join {user_id} to {room_alias_or_id}: {response.status_code} {response.text}"
    )
    return "failed"


async def sync_user_to_team_rooms_detailed(username: str, team_memberships: dict[int, bool]) -> tuple[list[dict[str, object]], list[str]]:
    """Ensure team rooms exist and return per-team sync details plus moderation failures."""
    if not team_memberships:
        return [], []

    results = []
    failed_moderation_rooms = []
    user_id = to_mxid(username)

    for team_id, is_organizer in sorted(team_memberships.items()):
        team_name = get_team_name(team_id)
        room_id = await ensure_team_room(team_id, team_name)
        if not room_id:
            results.append(
                {
                    "team_id": team_id,
                    "team_name": team_name,
                    "is_organizer": is_organizer,
                    "room_id": None,
                    "status": "failed",
                }
            )
            continue

        parent_space_ids = await get_room_parent_spaces(room_id)
        if parent_space_ids:
            spaces_joined, failed_spaces = await join_user_to_rooms(username, parent_space_ids)
            if not spaces_joined or failed_spaces:
                logger.warning(
                    "Failed to auto-join %s to parent spaces for team '%s': %s",
                    username,
                    team_name,
                    ", ".join(failed_spaces),
                )

        join_status = await join_user_to_room(username, room_id)
        if join_status == "failed":
            results.append(
                {
                    "team_id": team_id,
                    "team_name": team_name,
                    "is_organizer": is_organizer,
                    "room_id": room_id,
                    "status": "failed",
                }
            )
            continue

        if is_organizer:
            moderator_ok = await set_room_moderator(room_id, user_id)
            if not moderator_ok:
                failed_moderation_rooms.append(team_name)

        results.append(
            {
                "team_id": team_id,
                "team_name": team_name,
                "is_organizer": is_organizer,
                "room_id": room_id,
                "status": join_status,
            }
        )

    return results, failed_moderation_rooms


async def join_user_to_team_rooms(username: str, team_memberships: dict[int, bool]) -> tuple[bool, list[str], list[str]]:
    """Join user to all team rooms, creating them if needed, and grant organizer moderation."""
    results, failed_moderation_rooms = await sync_user_to_team_rooms_detailed(username, team_memberships)
    failed_team_rooms = [result["team_name"] for result in results if result["status"] == "failed"]
    return len(failed_team_rooms) == 0, failed_team_rooms, failed_moderation_rooms


async def join_user_to_rooms(username: str, room_aliases: list[str] | tuple[str, ...]) -> tuple[bool, list[str]]:
    """Join a local user to rooms using Synapse Admin API."""
    if not room_aliases:
        return True, []

    failed_rooms = []

    for room in room_aliases:
        join_status = await join_user_to_room(username, room)
        if join_status == "failed":
            failed_rooms.append(room)

    return len(failed_rooms) == 0, failed_rooms


def main() -> None:
    """Start the bot."""
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("register", register))
    application.add_handler(CommandHandler("my_teams", my_teams))
    application.add_handler(CommandHandler("reset_password", reset_password))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("hr_sync", ops_sync))
    application.add_handler(CommandHandler("hr_check", ops_check))
    application.add_handler(CommandHandler("hr_register", ops_register))
    application.add_handler(CommandHandler("hr_sync_teams", ops_sync_teams))
    application.add_error_handler(error_handler)

    # Run the bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    _retry_delay = 5
    while True:
        asyncio.set_event_loop(asyncio.new_event_loop())
        try:
            main()
            break
        except NetworkError as exc:
            logger.warning(
                "Startup network error (will retry in %ds): %s",
                _retry_delay,
                format_exception_chain(exc),
            )
            time.sleep(_retry_delay)
            _retry_delay = min(_retry_delay * 2, 60)
