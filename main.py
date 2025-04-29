import os
import sys
import asyncio
import logging
import signal
import asyncpg
import aiohttp
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import suppress
from functools import wraps
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN")
DASH_ADDRESSES = os.getenv("DASH_ADDRESSES", "").split(",")
DB_NAME = os.getenv("DB_NAME", "telegram_bot_db")
DB_USER = os.getenv("DB_USER", "bot_user")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
CASHBACK_PERCENT = 5
REFERRAL_BONUS = 10
MIN_DEPOSIT = 0.0001
RESERVATION_TIMEOUT = 1800
MAX_DB_RETRIES = 5
MEDIA_FOLDER = "/var/lib/telegram_bot/media"

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


class PaymentState(StatesGroup):
    waiting_for_payment = State()
    waiting_for_referral = State()
    admin_add_product = State()
    admin_remove_product = State()
    admin_select_district = State()
    admin_enter_product_name = State()
    admin_enter_product_price = State()
    admin_enter_product_description = State()
    admin_add_images = State()
    admin_create_promocode = State()
    admin_promocode_amount = State()
    admin_promocode_uses = State()
    admin_topup_user = State()
    admin_topup_amount = State()
    activate_promocode = State()


def db_retry(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(1, MAX_DB_RETRIES + 1):
            try:
                return await func(*args, **kwargs)
            except (asyncpg.PostgresConnectionError, asyncpg.QueryCanceledError) as e:
                last_exception = e
                logger.warning(f"DB error, retry {attempt}/{MAX_DB_RETRIES}")
                await asyncio.sleep(0.5 * attempt)
            except Exception as e:
                logger.error(f"DB error: {e}")
                raise
        logger.error(f"DB operation failed after {MAX_DB_RETRIES} attempts")
        raise last_exception or Exception("Database error")

    return wrapper


pool = None


async def create_db_pool():
    return await asyncpg.create_pool(
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )


@db_retry
async def init_db():
    global pool
    pool = await create_db_pool()
    Path(MEDIA_FOLDER).mkdir(parents=True, exist_ok=True)

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users(
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                username TEXT,
                balance NUMERIC DEFAULT 0 CHECK(balance >= 0),
                cashback NUMERIC DEFAULT 0,
                referral_code TEXT UNIQUE,
                referred_by BIGINT REFERENCES users(id),
                registration_date TIMESTAMP NOT NULL,
                last_activity TIMESTAMP NOT NULL,
                total_deposits NUMERIC DEFAULT 0,
                total_purchases NUMERIC DEFAULT 0
            )""")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions(
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id),
                amount NUMERIC NOT NULL,
                type TEXT NOT NULL,
                date TIMESTAMP NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'completed', 'failed'))
            )""")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS addresses(
                id SERIAL PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                status TEXT CHECK(status IN ('free', 'busy')) DEFAULT 'free',
                user_id BIGINT,
                reserved_time TIMESTAMP
            )""")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS districts(
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )""")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS products(
                id SERIAL PRIMARY KEY,
                district_id INTEGER NOT NULL REFERENCES districts(id),
                name TEXT NOT NULL,
                price NUMERIC NOT NULL,
                description TEXT
            )""")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS promocodes(
                code TEXT PRIMARY KEY,
                amount NUMERIC NOT NULL,
                max_uses INTEGER NOT NULL,
                remaining_uses INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL
            )""")

        districts = ['Arabkir', 'Kentron', 'Zeytun']
        for district in districts:
            await conn.execute("""
                INSERT INTO districts(name) 
                VALUES($1) 
                ON CONFLICT (name) DO NOTHING
            """, district)

        for addr in DASH_ADDRESSES:
            await conn.execute("""
                INSERT INTO addresses(address) 
                VALUES($1) 
                ON CONFLICT (address) DO NOTHING
            """, addr.strip())


def create_main_menu():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="👤 Profile", callback_data="profile"),
        InlineKeyboardButton(text="🏙️ Regions", callback_data="regions")
    )
    builder.row(
        InlineKeyboardButton(text="💰 Balance", callback_data="balance"),
        InlineKeyboardButton(text="👥 Referral", callback_data="referral")
    )
    return builder.as_markup()


def create_balance_menu():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="💳 Deposit", callback_data="deposit"),
        InlineKeyboardButton(text="⬅️ Back", callback_data="main")
    )
    return builder.as_markup()


@dp.message(Command("start", "help"))
async def cmd_start(message: types.Message, state: FSMContext):
    try:
        await state.clear()
        referred_by = None

        if len(message.text.split()) > 1:
            ref_code = message.text.split()[1]
            if ref_code.startswith("REF"):
                try:
                    ref_user_id = int(ref_code[3:])
                    if ref_user_id != message.from_user.id:
                        referred_by = ref_user_id
                except ValueError:
                    pass

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users 
                (id, name, username, registration_date, last_activity, referral_code)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                username = EXCLUDED.username,
                last_activity = EXCLUDED.last_activity
            """, message.from_user.id,
                               message.from_user.full_name,
                               message.from_user.username,
                               datetime.now(),
                               datetime.now(),
                               f"REF{message.from_user.id}")

            if referred_by:
                await conn.execute("""
                    UPDATE users SET balance = balance + $1 
                    WHERE id = $2
                """, REFERRAL_BONUS, referred_by)
                await conn.execute("""
                    INSERT INTO transactions 
                    (user_id, amount, type, date, status)
                    VALUES ($1, $2, 'referral', $3, 'completed')
                """, referred_by, REFERRAL_BONUS, datetime.now())

        await message.answer(
            f"👋 Hello, {message.from_user.full_name}!",
            reply_markup=create_main_menu()
        )

    except Exception as e:
        logger.error(f"Start error: {e}")
        await message.answer("❌ Registration failed. Please try again.")


@dp.callback_query(F.data == "main")
async def handle_main_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Main menu:",
        reply_markup=create_main_menu()
    )


@dp.callback_query(F.data == "balance")
async def handle_balance(callback: types.CallbackQuery):
    try:
        async with pool.acquire() as conn:
            balance = await conn.fetchval(
                "SELECT balance FROM users WHERE id = $1",
                callback.from_user.id
            )

        await callback.message.edit_text(
            f"💰 Your Balance: ${balance:.2f}",
            reply_markup=create_balance_menu()
        )

    except Exception as e:
        logger.error(f"Balance error: {e}")
        await callback.answer("Error loading balance", show_alert=True)


@dp.callback_query(F.data == "deposit")
async def handle_deposit(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        async with pool.acquire() as conn:
            address = await conn.fetchval("""
                SELECT address FROM addresses 
                WHERE status = 'free' 
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """)

            if not address:
                await callback.answer("All addresses busy. Try later.", show_alert=True)
                return

            await conn.execute("""
                UPDATE addresses 
                SET status = 'busy', 
                    user_id = $1, 
                    reserved_time = $2 
                WHERE address = $3
            """, user_id, datetime.now(), address)

        msg = await callback.message.edit_text(
            f"💳 Deposit to:\n<code>{address}</code>\n\n"
            f"⚠️ Send ONLY DASH to this address\n"
            f"⏳ Address reserved for 30 minutes",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Copy Address", callback_data="copy_address"),
                 InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_deposit")]
            ])
        )
        await state.update_data(
            message_id=msg.message_id,
            deposit_address=address
        )
        await state.set_state(PaymentState.waiting_for_payment)
        asyncio.create_task(monitor_payment(callback, state))

    except Exception as e:
        logger.error(f"Deposit error: {e}")
        await callback.answer("Error starting deposit", show_alert=True)


async def monitor_payment(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    address = data['deposit_address']
    user_id = callback.from_user.id

    try:
        initial_utxo = await get_unspent_transactions(address)
        initial_balance = sum(tx['amount'] for tx in initial_utxo) if initial_utxo else 0

        for _ in range(RESERVATION_TIMEOUT // 5):
            if await state.get_state() != PaymentState.waiting_for_payment:
                return

            await asyncio.sleep(5)

            current_utxo = await get_unspent_transactions(address)
            current_balance = sum(tx['amount'] for tx in current_utxo) if current_utxo else 0

            if current_balance > initial_balance:
                amount = current_balance - initial_balance
                if amount >= MIN_DEPOSIT:
                    async with pool.acquire() as conn:
                        await conn.execute("""
                            UPDATE users 
                            SET balance = balance + $1,
                                total_deposits = total_deposits + $1
                            WHERE id = $2
                        """, amount, user_id)

                        await conn.execute("""
                            INSERT INTO transactions 
                            (user_id, amount, type, date, status)
                            VALUES ($1, $2, 'deposit', $3, 'completed')
                        """, user_id, amount, datetime.now())

                        await conn.execute("""
                            UPDATE addresses 
                            SET status = 'free',
                                user_id = NULL,
                                reserved_time = NULL 
                            WHERE address = $1
                        """, address)

                    dash_price = await get_dash_price() or 0
                    await callback.message.answer(
                        f"✅ Deposit received!\n"
                        f"Amount: {amount:.8f} DASH (≈${amount * dash_price:.2f})",
                        reply_markup=create_balance_menu()
                    )
                    await state.clear()
                    return

        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE addresses 
                SET status = 'free',
                    user_id = NULL,
                    reserved_time = NULL 
                WHERE address = $1
            """, address)

        await callback.message.answer(
            "Payment not detected. Address released",
            reply_markup=create_back_button("balance")
        )
        await state.clear()

    except Exception as e:
        logger.error(f"Payment monitoring error: {e}")
        await callback.message.answer(
            "Payment check error. Address released",
            reply_markup=create_back_button("balance")
        )
        await state.clear()


async def get_unspent_transactions(address: str):
    url = f"{os.getenv('INSIGHT_API_URL', 'https://insight.dash.org/insight-api')}/addr/{address}/utxo"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                return [{
                    'txid': tx['txid'],
                    'amount': tx['amount'],
                    'confirmations': tx['confirmations'],
                    'time': tx['time'] if 'time' in tx else datetime.now().timestamp()
                } for tx in data]
    except Exception as e:
        logger.error(f"Insight API error: {e}")
        return []


async def get_dash_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=dash&vs_currencies=usd"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                return data['dash']['usd']
    except Exception as e:
        logger.error(f"CoinGecko API error: {e}")
        return None


@dp.callback_query(F.data == "cancel_deposit")
async def handle_cancel_deposit(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if 'deposit_address' in data:
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE addresses 
                SET status = 'free',
                    user_id = NULL,
                    reserved_time = NULL 
                WHERE address = $1
            """, data['deposit_address'])
    await state.clear()
    await callback.message.edit_text(
        "Deposit canceled",
        reply_markup=create_balance_menu()
    )


@dp.callback_query(F.data == "copy_address")
async def handle_copy_address(callback: types.CallbackQuery):
    data = await callback.state.get_data()
    address = data.get('deposit_address')
    if address:
        await callback.answer(f"Address copied!\n{address}", show_alert=True)


@dp.callback_query(F.data == "profile")
async def handle_profile(callback: types.CallbackQuery):
    try:
        async with pool.acquire() as conn:
            user_data = await conn.fetchrow("""
                SELECT balance, cashback, total_deposits, total_purchases 
                FROM users WHERE id = $1
            """, callback.from_user.id)

            referrals_count = await conn.fetchval("""
                SELECT COUNT(*) FROM users WHERE referred_by = $1
            """, callback.from_user.id)

        if user_data:
            text = (
                f"👤 <b>Your Profile</b>\n\n"
                f"🆔 ID: <code>{callback.from_user.id}</code>\n"
                f"💰 Balance: ${user_data['balance']:.2f}\n"
                f"🔄 Cashback: ${user_data['cashback']:.2f}\n"
                f"💵 Total Deposits: ${user_data['total_deposits']:.2f}\n"
                f"🛍️ Total Purchases: ${user_data['total_purchases']:.2f}\n"
                f"👥 Referrals: {referrals_count}"
            )
            await callback.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🎫 Activate Promo", callback_data="activate_promo"),
                     InlineKeyboardButton(text="⬅️ Back", callback_data="main")]
                ])
            )
    except Exception as e:
        logger.error(f"Profile error: {e}")
        await callback.answer("Error loading profile", show_alert=True)


async def graceful_shutdown(sig=None):
    logger.info(f"Shutting down...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    if pool:
        await pool.close()


async def main():
    await init_db()

    loop = asyncio.get_running_loop()

    if sys.platform != 'win32':
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(graceful_shutdown(s))
            )

    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        logger.info("Bot stopped gracefully")
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())