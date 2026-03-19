from __future__ import annotations

import os
import logging
import asyncio
import httpx
import hmac
import hashlib
import secrets
import time
from urllib.parse import quote
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import NetworkError
from telegram.helpers import escape_markdown
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
FAKE_TEST_ROOM_ALIASES = (
    '#fake-1:insomniafest.ru',
    '#fake-2:insomniafest.ru',
)
ORGS_ROOM = '#orgs:insomniafest.ru'
GRIST_DOC_ID = "mhwDM83vLmT3"
GRIST_TABLE_ID = "Participations"
GRIST_TEAMS_TABLE_ID = "Teams"
TEAM_ROOM_MODERATOR_LEVEL = 50

GRIST_API_KEY = os.getenv('GRIST_API_KEY')
OWNER_TELEGRAM_ID_RAW = os.getenv('OWNER_TELEGRAM_ID')
ADMIN_TELEGRAM_IDS_RAW = os.getenv('ADMIN_TELEGRAM_IDS', '')

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

if ADMIN_TELEGRAM_IDS_RAW:
    for raw_id in ADMIN_TELEGRAM_IDS_RAW.split(','):
        raw_id = raw_id.strip()
        if not raw_id:
            continue
        try:
            ADMIN_TELEGRAM_IDS.add(int(raw_id))
        except ValueError:
            logger.error(f"Invalid ADMIN_TELEGRAM_IDS value skipped: {raw_id}")

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
grist_team_id_to_name = {}
grist_max_record_id = 0
grist_last_full_sync = 0.0

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

            try:
                record_id = int(record_id)
            except (TypeError, ValueError):
                continue

            normalized = normalize_telegram_handle(telegram2)
            if not normalized:
                continue

            grist_handle_to_record_id[normalized] = record_id
            if isinstance(person_name, str) and person_name.strip():
                grist_handle_to_person_name[normalized] = person_name.strip()
            else:
                grist_handle_to_person_name.pop(normalized, None)

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


async def post_init(application: Application) -> None:
    """Notify owner that bot started and basic configuration is loaded."""
    if OWNER_TELEGRAM_ID is None:
        logger.warning("OWNER_TELEGRAM_ID is not set; owner notifications are disabled")
    sync_ok = await sync_grist_cache(force_full=True)
    if not sync_ok:
        logger.critical("Initial Grist cache sync failed, stopping bot startup")
        raise RuntimeError("Initial Grist cache sync failed")

    if OWNER_TELEGRAM_ID is None:
        return

    try:
        cache_status = (
            f"Кэш Grist: {len(grist_handle_to_record_id)} пользователей, max_id={grist_max_record_id}"
            if sync_ok
            else "Кэш Grist: не удалось обновить при старте"
        )
        await application.bot.send_message(
            chat_id=OWNER_TELEGRAM_ID,
            text=f"✅ Бот запущен. Уведомления об ошибках активны.\n{cache_status}",
        )
    except Exception as e:
        logger.error(f"Failed to send startup notification to owner: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    message = """
Привет! Я бот для регистрации в чате для волонтеров. Чтобы зарегистрироваться, отправьте команду /register.

Важно: ваш аккаунт в нашем чате будет создан с тем же именем пользователя, что и тут в Telegram, чтобы ваши друзья и коллеги могли легко вас найти.
    """
    await update.message.reply_text(message)


async def register(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle registration request."""
    user_id = update.effective_user.id
    username = normalize_telegram_handle(update.effective_user.username) or str(user_id)
    
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
        is_eligible, eligibility_check_ok, person_name, team_memberships = await check_user_eligibility(username)

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
                        "⚠️ Не удалось ре-активировать пользователя\n"
                        f"username={username}\n"
                        f"reactivation_error={reactivation_error_code}"
                    ),
                )
        
        if success:
            if person_name:
                displayname_ok = await set_synapse_display_name(username, person_name)
                if not displayname_ok:
                    logger.warning(f"Could not set display name for {username} to '{person_name}'")

            room_aliases = list(AUTO_JOIN_ROOMS)
            if is_organizer:
                room_aliases.append(ORGS_ROOM)

            join_ok, failed_rooms = await join_user_to_rooms(username, room_aliases)
            team_join_ok, failed_team_rooms, failed_moderation_rooms = await join_user_to_team_rooms(
                username,
                team_memberships,
            )
            escaped_username = escape_markdown(username, version=1)
            escaped_password = escape_markdown(temp_password, version=1)
            message = f"""✅ Поздравляем!

Вы можете войти в чат для волонтеров, используя следующие учетные данные:

**Имя пользователя:** {escaped_username}
**Временный пароль:** {escaped_password} (поменяйте его при первом входе)

🔗 **Ссылка на чат:** {ELEMENT_URL}
📖 **Помощь:** {HELP_URL}
            """
            if account_reactivated:
                message += "\n♻️ Ваш аккаунт был восстановлен после деактивации."
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

            if not join_ok and failed_rooms:
                await update.message.reply_text(
                    "⚠️ Аккаунт создан, но не удалось автоматически добавить вас в комнаты: "
                    f"{', '.join(failed_rooms)}.\n"
                    "Попросите администраторов отправить вам инвайт в эти комнаты."
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
                    "⚠️ Вы добавлены в командные комнаты, но не удалось выдать права модератора в: "
                    f"{', '.join(failed_moderation_rooms)}."
                )

                await notify_owner(
                    context,
                    (
                        "⚠️ Не удалось выдать права модератора в командных комнатах\n"
                        f"username={username}\n"
                        f"failed_moderation_rooms={', '.join(failed_moderation_rooms)}"
                    ),
                )
        elif registration_error_code == "M_USER_IN_USE":
            await update.message.reply_text(
                f"""⚠️ Похоже, учетная запись "{username}" уже существует.

🔗 Попробуйте войти тут: {ELEMENT_URL}
📖 Помощь: {HELP_URL}

Если не получается войти, напишите администраторам для сброса пароля."""
            )
        else:
            await update.message.reply_text("❌ Не удалось создать учетную запись. Пожалуйста, попробуйте позже.")
            
    except Exception as e:
        logger.error(f"Registration error for user {user_id}: {e}")
        await notify_owner(
            context,
            f"⚠️ Ошибка регистрации\nuser_id={user_id}\nusername={username}\nerror={e}",
        )
        await update.message.reply_text("❌ Произошла ошибка при регистрации. Пожалуйста, попробуйте позже.")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle unexpected telegram framework errors and alert owner."""
    if isinstance(context.error, NetworkError):
        logger.warning("Transient network error (will retry): %s", context.error)
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
        f"Перейдите на страницу помощи: {HELP_URL}\n\n"
        "Если возникнут вопросы или проблемы, напишите администраторам."
    )

    if is_admin_telegram_user(update):
        message += (
            "\n\n"
            "🔐 Команды админов (скрытые)\n\n"
            "/ops_sync - принудительно обновить кэш Grist и показать счетчики.\n"
            "/ops_check @handle - проверить eligibility и членство по командам.\n"
            "/ops_register @handle - выполнить полную регистрацию: Matrix-аккаунт, автодобавление в комнаты и командные комнаты.\n"
            "/ops_fake_register fake-1 [#fake-1 #fake-2] - создать тестового Matrix-пользователя и проверить автодобавление в комнаты."
        )

    await update.message.reply_text(message)


def is_admin_telegram_user(update: Update) -> bool:
    """Check whether Telegram user is allowed to run hidden ops commands."""
    if not update or not update.effective_user:
        return False
    return update.effective_user.id in ADMIN_TELEGRAM_IDS


async def require_admin(update: Update) -> bool:
    """Return True for admin users, otherwise send denial message."""
    if is_admin_telegram_user(update):
        return True

    await update.message.reply_text("❌ Эта команда недоступна.")
    return False


async def ops_sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Force Grist cache sync and report counters. Hidden admin-only command."""
    if not await require_admin(update):
        return

    ok = await sync_grist_cache(force_full=True)
    if not ok:
        await update.message.reply_text("❌ Sync failed")
        return

    await update.message.reply_text(
        (
            "✅ Sync complete\n"
            f"users={len(grist_handle_to_record_id)}\n"
            f"teams={len(grist_team_id_to_name)}\n"
            f"max_record_id={grist_max_record_id}"
        )
    )


async def ops_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check eligibility and team memberships for a Telegram handle. Hidden admin-only command."""
    if not await require_admin(update):
        return

    if not context.args:
        await update.message.reply_text("Usage: /ops_check <telegram_handle>")
        return

    handle = context.args[0]
    eligible, check_ok, person_name, memberships = await check_user_eligibility(handle)

    if not check_ok:
        await update.message.reply_text("❌ Eligibility check failed")
        return

    if not eligible:
        await update.message.reply_text(f"❌ Not eligible: {handle}")
        return

    lines = [
        "✅ Eligible",
        f"handle={normalize_telegram_handle(handle)}",
        f"person_name={person_name or '-'}",
    ]
    if memberships:
        for team_id, is_org in sorted(memberships.items()):
            lines.append(
                f"team={team_id} name={get_team_name(team_id)} organizer={str(is_org).lower()}"
            )
    else:
        lines.append("team_memberships=none")

    await update.message.reply_text("\n".join(lines))


async def ops_register(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run full registration flow for a provided handle. Hidden admin-only command."""
    if not await require_admin(update):
        return

    if not context.args:
        await update.message.reply_text("Usage: /ops_register <telegram_handle>")
        return

    handle = context.args[0]
    eligible, check_ok, person_name, memberships = await check_user_eligibility(handle)

    if not check_ok:
        await update.message.reply_text("❌ Eligibility check failed")
        return

    if not eligible:
        await update.message.reply_text(f"❌ Not eligible: {handle}")
        return

    username = normalize_telegram_handle(handle)
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
                f"❌ Reactivation failed for {username}: {reactivation_error}"
            )
            return

    created = register_ok and not reactivated
    if not register_ok and registration_error != "M_USER_IN_USE":
        await update.message.reply_text(
            f"❌ Registration failed for {username}: {registration_error}"
        )
        return

    displayname_ok = True
    if person_name:
        displayname_ok = await set_synapse_display_name(username, person_name)

    room_aliases = list(AUTO_JOIN_ROOMS)
    if is_organizer:
        room_aliases.append(ORGS_ROOM)

    join_ok, failed_rooms = await join_user_to_rooms(username, room_aliases)
    team_join_ok, failed_team_rooms, failed_moderation_rooms = await join_user_to_team_rooms(
        username,
        memberships,
    )

    lines = [
        "🧪 Admin full registration",
        f"handle={username}",
        f"mxid={to_mxid(username)}",
        f"person_name={person_name or '-'}",
        f"created={str(created).lower()}",
        f"reactivated={str(reactivated).lower()}",
        f"displayname_updated={str(displayname_ok).lower()}",
        f"default_join_ok={str(join_ok).lower()}",
        f"team_join_ok={str(team_join_ok).lower()}",
    ]

    if created:
        lines.append(f"temp_password={temp_password}")

    if failed_rooms:
        lines.append(f"failed_rooms={', '.join(failed_rooms)}")

    if failed_team_rooms:
        lines.append(f"failed_team_rooms={', '.join(failed_team_rooms)}")

    if failed_moderation_rooms:
        lines.append(f"failed_moderation_rooms={', '.join(failed_moderation_rooms)}")

    await update.message.reply_text("\n".join(lines))


def normalize_room_alias(alias: str) -> str:
    """Normalize room alias and append server name when domain is omitted."""
    value = (alias or "").strip()
    if not value:
        return value

    if not value.startswith('#'):
        value = f"#{value}"

    if ':' not in value:
        value = f"{value}:{SYNAPSE_SERVER_NAME}"

    return value


def sanitize_fake_localpart(raw_value: str) -> str:
    """Sanitize user-provided localpart for fake registration command."""
    value = (raw_value or "").strip()
    if value.startswith('@'):
        value = value[1:]
    if ':' in value:
        value = value.split(':', 1)[0]
    return value


async def ops_fake_register(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Create/join a fake Matrix user to test rooms. Hidden admin-only command."""
    if not await require_admin(update):
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /ops_fake_register <fake-localpart> [#room1 #room2 ...]"
        )
        return

    localpart = sanitize_fake_localpart(context.args[0]).lower()
    if not localpart:
        await update.message.reply_text("❌ Empty localpart")
        return

    if not localpart.startswith('fake-'):
        await update.message.reply_text("❌ For safety, localpart must start with 'fake-'")
        return

    requested_rooms = context.args[1:] if len(context.args) > 1 else []
    if requested_rooms:
        rooms = [normalize_room_alias(room) for room in requested_rooms if normalize_room_alias(room)]
    else:
        rooms = list(FAKE_TEST_ROOM_ALIASES)

    if not rooms:
        await update.message.reply_text("❌ No valid room aliases provided")
        return

    temp_password = secrets.token_urlsafe(12)
    register_ok, registration_error = await register_synapse_user(localpart, temp_password)

    created = register_ok
    if not register_ok and registration_error != "M_USER_IN_USE":
        await update.message.reply_text(
            f"❌ Registration failed for {localpart}: {registration_error}"
        )
        return

    join_ok, failed_rooms = await join_user_to_rooms(localpart, rooms)

    lines = [
        "🧪 Fake registration smoke test",
        f"mxid={to_mxid(localpart)}",
        f"created={str(created).lower()}",
        f"rooms={', '.join(rooms)}",
        f"join_ok={str(join_ok).lower()}",
    ]

    if created:
        lines.append(f"temp_password={temp_password}")

    if failed_rooms:
        lines.append(f"failed_rooms={', '.join(failed_rooms)}")

    await update.message.reply_text("\n".join(lines))


async def check_user_eligibility(telegram_handle: str) -> tuple[bool, bool, str | None, dict[int, bool]]:
    """Return (is_eligible, check_ok, person_name, team_memberships) using in-memory Grist cache."""
    try:
        if not telegram_handle:
            logger.warning("Empty telegram handle provided")
            return False, True, None, {}

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
            # Step 1: Get nonce
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
            
            # Step 2: Compute HMAC-SHA1
            admin_flag = "notadmin"
            msg = "\x00".join([nonce, username, password, admin_flag]).encode("utf-8")
            secret_bytes = SYNAPSE_ADMIN_TOKEN.encode("utf-8")
            mac = hmac.new(secret_bytes, msg, hashlib.sha1).hexdigest()
            
            # Step 3: Register user
            payload = {
                "nonce": nonce,
                "username": username,
                "password": password,
                "admin": False,
                "mac": mac
            }
            
            register_response = await request_with_retries(
                client,
                "POST",
                f"{SYNAPSE_API_URL}/_synapse/admin/v1/register",
                json=payload,
            )
            
            if register_response.status_code == 200:
                logger.info(f"User {username} registered successfully")
                return True, None
            else:
                error_code = None
                try:
                    error_code = register_response.json().get("errcode")
                except Exception:
                    error_code = None

                if register_response.status_code == 400 and error_code == "M_USER_IN_USE":
                    logger.warning(f"User {username} already exists in Synapse")
                    return False, "M_USER_IN_USE"

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
    """Set room power level to moderator for selected user."""
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
            if isinstance(current_level, int) and current_level >= TEAM_ROOM_MODERATOR_LEVEL:
                return True

            users[user_id] = TEAM_ROOM_MODERATOR_LEVEL
            put_response = await request_with_retries(
                client,
                "PUT",
                power_levels_url,
                headers=headers,
                json=payload,
            )

        if put_response.status_code not in (200, 201):
            logger.warning(
                f"Could not set moderator for {user_id} in {room_id}: "
                f"{put_response.status_code} {put_response.text}"
            )
            return False

        return True
    except Exception as e:
        logger.warning(f"Could not set moderator for {user_id} in {room_id}: {e}")
        return False


async def join_user_to_team_rooms(username: str, team_memberships: dict[int, bool]) -> tuple[bool, list[str], list[str]]:
    """Join user to all team rooms, creating them if needed, and grant organizer moderation."""
    if not team_memberships:
        return True, [], []

    failed_team_rooms = []
    failed_moderation_rooms = []
    user_id = to_mxid(username)

    for team_id, is_organizer in sorted(team_memberships.items()):
        team_name = get_team_name(team_id)
        room_id = await ensure_team_room(team_id, team_name)
        if not room_id:
            failed_team_rooms.append(team_name)
            continue

        joined, failed_rooms = await join_user_to_rooms(username, [room_id])
        if not joined or failed_rooms:
            failed_team_rooms.append(team_name)
            continue

        if is_organizer:
            moderator_ok = await set_room_moderator(room_id, user_id)
            if not moderator_ok:
                failed_moderation_rooms.append(team_name)

    return len(failed_team_rooms) == 0, failed_team_rooms, failed_moderation_rooms


async def join_user_to_rooms(username: str, room_aliases: list[str] | tuple[str, ...]) -> tuple[bool, list[str]]:
    """Join a local user to rooms using Synapse Admin API."""
    if not room_aliases:
        return True, []

    if not SYNAPSE_ADMIN_ACCESS_TOKEN:
        logger.warning("SYNAPSE_ADMIN_ACCESS_TOKEN is not set; skipping auto-join to rooms")
        return False, list(room_aliases)

    user_id = to_mxid(username)
    headers = {
        "Authorization": f"Bearer {SYNAPSE_ADMIN_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    failed_rooms = []

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            for room in room_aliases:
                room_id_or_alias = quote(room, safe='')
                response = await request_with_retries(
                    client,
                    "POST",
                    f"{SYNAPSE_API_URL}/_synapse/admin/v1/join/{room_id_or_alias}",
                    headers=headers,
                    json={"user_id": user_id},
                )

                if response.status_code not in (200, 201):
                    failed_rooms.append(room)
                    logger.warning(
                        f"Failed to auto-join {user_id} to {room}: {response.status_code} {response.text}"
                    )

    except Exception as e:
        logger.error(f"Auto-join request failed for {user_id}: {e}")
        return False, list(room_aliases)

    return len(failed_rooms) == 0, failed_rooms


def main() -> None:
    """Start the bot."""
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("register", register))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("ops_sync", ops_sync))
    application.add_handler(CommandHandler("ops_check", ops_check))
    application.add_handler(CommandHandler("ops_register", ops_register))
    application.add_handler(CommandHandler("ops_fake_register", ops_fake_register))
    application.add_error_handler(error_handler)

    # Run the bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
