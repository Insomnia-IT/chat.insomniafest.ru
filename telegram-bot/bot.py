import os
import logging
import asyncio
import httpx
import hmac
import hashlib
import secrets
import time
from telegram import Update
from telegram.constants import ParseMode
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
SYNAPSE_API_URL = "https://synapse.insomniafest.ru"
ELEMENT_URL = "https://chat.insomniafest.ru"
HELP_URL = "https://chat.insomniafest.ru/help"
GRIST_DOC_ID = "mhwDM83vLmT3"
GRIST_TABLE_ID = "Participations"

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

# Rate limiting: track last registration attempt per user (user_id -> timestamp)
REGISTRATION_RATE_LIMIT = 300  # 5 minutes in seconds
user_registration_times = {}

# HTTP settings for external APIs
HTTP_TIMEOUT = httpx.Timeout(timeout=15.0, connect=5.0)
HTTP_RETRIES = 2

# Grist eligibility cache
GRIST_ALLOWED_STATUS_CODES = ("PLANNED", "STARTED", "COMPLETE")
GRIST_CACHE_FULL_SYNC_INTERVAL = 600  # seconds
grist_sql_available = True
grist_cache_lock = asyncio.Lock()
grist_handle_to_record_id = {}
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


def normalize_telegram_handle(handle: str) -> str:
    """Normalize Telegram handles for case-insensitive matching."""
    return handle.lstrip('@').strip().lower()


def build_grist_sql_query(min_record_id: int = 0) -> str:
    """Build SQL for selecting eligible volunteer handles from Grist."""
    statuses = ", ".join(f"'{status}'" for status in GRIST_ALLOWED_STATUS_CODES)
    min_id_clause = f"AND id > {min_record_id}" if min_record_id > 0 else ""
    return (
        "SELECT id, Telegram2 "
        f"FROM {GRIST_TABLE_ID} "
        "WHERE year = 2026 "
        f"AND status_code IN ({statuses}) "
        "AND Telegram2 IS NOT NULL "
        "AND Telegram2 != '' "
        f"{min_id_clause} "
        "ORDER BY id ASC"
    )


async def fetch_grist_records_sql(query: str) -> list:
    """Run a read-only SQL query against Grist and return records list."""
    url = f"https://grist.insomniafest.ru/api/docs/{GRIST_DOC_ID}/sql"
    headers = {
        "Authorization": f"Bearer {GRIST_API_KEY}",
        "Content-Type": "application/json",
    }
    params = {"q": query}

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await request_with_retries(
            client,
            "GET",
            url,
            params=params,
            headers=headers,
        )

    if response.status_code != 200:
        raise RuntimeError(f"Grist SQL API error: {response.status_code} {response.text}")

    data = response.json()
    return data.get("records", [])


async def fetch_grist_records_via_records_api() -> list:
    """Fetch eligible records using Grist records API as a fallback."""
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


async def sync_grist_cache(force_full: bool = False) -> bool:
    """Sync eligibility cache from Grist (incremental by default)."""
    global grist_max_record_id, grist_last_full_sync, grist_sql_available

    async with grist_cache_lock:
        now = time.time()
        do_full_sync = (
            force_full
            or not grist_handle_to_record_id
            or (now - grist_last_full_sync) >= GRIST_CACHE_FULL_SYNC_INTERVAL
        )

        min_record_id = 0 if do_full_sync else grist_max_record_id
        records = []
        incremental_used = False

        if grist_sql_available:
            query = build_grist_sql_query(min_record_id=min_record_id)
            try:
                records = await fetch_grist_records_sql(query)
                incremental_used = True
            except Exception as e:
                err_text = str(e)
                if "403" in err_text or "insufficient document access" in err_text.lower():
                    grist_sql_available = False
                    logger.warning("Grist SQL API is unavailable for this token, switching to records API fallback")
                else:
                    logger.error(f"Failed to sync Grist cache: {e}")
                    return False

        if not grist_sql_available:
            try:
                records = await fetch_grist_records_via_records_api()
                incremental_used = False
            except Exception as e:
                logger.error(f"Failed to sync Grist cache via records API: {e}")
                return False

        if do_full_sync or not incremental_used:
            grist_handle_to_record_id.clear()
            grist_max_record_id = 0

        for record in records:
            fields = record.get("fields", {})
            record_id = fields.get("id")
            if record_id is None:
                record_id = record.get("id")
            telegram2 = fields.get("Telegram2")
            if not telegram2:
                continue

            try:
                record_id = int(record_id)
            except (TypeError, ValueError):
                continue

            normalized = normalize_telegram_handle(telegram2)
            if not normalized:
                continue

            grist_handle_to_record_id[normalized] = record_id
            if record_id > grist_max_record_id:
                grist_max_record_id = record_id

        if do_full_sync or not incremental_used:
            grist_last_full_sync = now
            logger.info(
                f"Grist full sync complete: {len(grist_handle_to_record_id)} handles, max_id={grist_max_record_id}"
            )
        else:
            logger.info(
                f"Grist incremental sync complete: +{len(records)} rows, max_id={grist_max_record_id}"
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
    """
    await update.message.reply_text(message)


async def register(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle registration request."""
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    
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
        is_eligible, eligibility_check_ok = await check_user_eligibility(username)

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
        success = await register_synapse_user(username, temp_password)
        
        if success:
            escaped_username = escape_markdown(username, version=1)
            escaped_password = escape_markdown(temp_password, version=1)
            message = f"""✅ Поздравляем!

Вы можете войти в чат для волонтеров, используя следующие учетные данные:

**Имя пользователя:** {escaped_username}
**Временный пароль:** {escaped_password} (поменяйте его при первом входе)

🔗 **Ссылка на чат:** {ELEMENT_URL}
📖 **Помощь:** {HELP_URL}
            """
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
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
    message = f"""
📖 **Помощь и документация**

Перейдите на страницу помощи: {HELP_URL}

Если возникнут вопросы или проблемы, напишите администраторам.
    """
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)


async def check_user_eligibility(telegram_handle: str) -> tuple[bool, bool]:
    """Return (is_eligible, check_ok) using in-memory Grist cache."""
    try:
        if not telegram_handle:
            logger.warning("Empty telegram handle provided")
            return False, True

        handle = normalize_telegram_handle(telegram_handle)

        # Fast path: do not call Grist if the handle is already in cache.
        if handle in grist_handle_to_record_id:
            logger.info(
                f"User {handle} found in Grist cache (record_id={grist_handle_to_record_id[handle]})"
            )
            return True, True

        # Cache miss: try syncing once, then re-check.
        sync_ok = await sync_grist_cache(force_full=False)

        # If Grist is temporarily unavailable, keep serving from stale cache.
        if not sync_ok and not grist_handle_to_record_id:
            logger.warning("Grist cache unavailable and empty")
            return False, False

        if handle in grist_handle_to_record_id:
            logger.info(
                f"User {handle} found in Grist cache after sync (record_id={grist_handle_to_record_id[handle]})"
            )
            return True, True

        logger.warning(f"User {handle} not found in Grist cache")
        return False, True
            
    except Exception as e:
        logger.error(f"Error checking eligibility for {telegram_handle}: {e}")
        return False, False


async def register_synapse_user(username: str, password: str) -> bool:
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
                return False
            nonce_data = nonce_response.json()
            nonce = nonce_data.get('nonce')
            
            if not nonce:
                logger.error(f"Failed to obtain nonce for {username}")
                return False
            
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
                return True
            else:
                logger.error(f"Failed to register user {username}: {register_response.status_code} {register_response.text}")
                return False
                
    except Exception as e:
        logger.error(f"Error registering user {username}: {e}")
        return False


def main() -> None:
    """Start the bot."""
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("register", register))
    application.add_handler(CommandHandler("help", help_command))
    application.add_error_handler(error_handler)

    # Run the bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
