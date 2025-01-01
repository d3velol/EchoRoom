from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import User, init_db, PrisonUser
from messages_db import StoredMessage, init_messages_db
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, UserDeactivated
import config
import random
import asyncio
import names
import time
import traceback
import json
from restricted_names import is_name_allowed
from datetime import datetime

# Инициализация бота
bot = Bot(token=config.BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

active_channels = set()

# Добавим словарь для хранения времени последнего сообщения
last_message_time = {}

# Глобальный словарь для хранения соответствий ID сообщений
message_mappings = {}  # {channel: {original_id: {user_id: message_id}}}

def owner_only(func):
    async def wrapper(message: types.Message):
        if message.from_user.id != config.OWNER_ID:
            await message.answer(
                "🚫 *Ошибка*: Недостаточно прав",
                parse_mode="Markdown"
            )
            return
        return await func(message)
    return wrapper

def get_current_version():
    try:
        with open('version.txt', 'r') as f:
            return f.read().strip()
    except:
        return "1.0.0"

def generate_name():
    return f"{names.get_first_name()} {names.get_last_name()}"

def get_random_channel():
    """Получить случайный канал"""
    if random.random() < config.CHANNEL_CREATION_CHANCE:
        new_channel = random.randint(config.MIN_CHANNEL, config.MAX_CHANNEL)
        active_channels.add(new_channel)
        return new_channel
    return random.choice(list(active_channels)) if active_channels else random.randint(config.MIN_CHANNEL, config.MAX_CHANNEL)

def get_display_name(user: User):
    """Получить отображаемое имя пользователя"""
    display_name = user.custom_name if user.custom_name else user.name
    if user.emoji:
        display_name = f"{user.emoji} {display_name}"
    return display_name

def create_user_button(name, user=None):
    """Создать кнопку с именем пользователя"""
    markup = InlineKeyboardMarkup()
    display_name = get_display_name(user) if user else name
    
    # Определяем callback_data в зависимости от типа кнопки
    if name == "Система":
        callback_data = "system"
    elif name == "Владелец":
        callback_data = "owner"
    else:
        callback_data = "name"
        
    markup.add(InlineKeyboardButton(display_name, callback_data=callback_data))
    return markup

@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    """
    Глобальный обработчик ошибок
    """
    print(f"\n{'='*50}\n[ERROR] Unexpected error: {exception}")
    print(f"Update: {update}")
    print(traceback.format_exc())
    print(f"{'='*50}\n")
    return True

async def send_message(**kwargs):
    """Универсальная функция отправки сообщения"""
    try:
        return await bot.send_message(**kwargs)
    except (BotBlocked, ChatNotFound, UserDeactivated) as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Failed to send message: {e}")
        print(f"Arguments: {kwargs}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        
        user_id = kwargs.get('chat_id')
        if user_id:
            user = User.get_user(user_id)
            if user:
                user.delete_instance()
            
            prison_user = PrisonUser.get_or_none(PrisonUser.user_id == user_id)
            if prison_user:
                prison_user.delete_instance()
        raise e
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Unexpected error while sending message: {e}")
        print(f"Arguments: {kwargs}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        raise e

async def start_channel_switchers():
    """Запускает таймеры переключения каналов"""
    users = User.select()
    for user in users:
        asyncio.create_task(switch_channel(user.user_id))
    print(f"Started channel switchers for {len(list(users))} users")

async def switch_channel(user_id):
    """Автоматическое переключение канала для одного пользователя"""
    while True:
        wait_time = random.randint(config.SWITCH_TIME_MIN, config.SWITCH_TIME_MAX)
        await asyncio.sleep(wait_time)
        
        user = User.get_user(user_id)
        if user:
            prison_user = PrisonUser.get_or_none(PrisonUser.user_id == user_id)
            if prison_user:
                continue
                
            new_channel = get_random_channel()
            new_name = generate_name()
            
            # Обновляем только если нет кастомного имени
            if not user.custom_name:
                user.name = new_name
            user.channel = new_channel
            user.save()
            
            try:
                await bot.send_message(
                    user_id,
                    f"👁 *Моргнув*, ты оказался на канале `{new_channel}Hz`\n"
                    f"🎭 *Твоё имя*: `{get_display_name(user)}`",
                    parse_mode="Markdown",
                    reply_markup=create_user_button(new_name, user)
                )
            except Exception as e:
                print(f"\n{'='*50}")
                print(f"[ERROR] Failed to send channel switch message to {user_id}: {e}")
                print(traceback.format_exc())
                print(f"{'='*50}\n")

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    try:
        user = User.get_or_none(User.user_id == message.from_user.id)
        if not user:
            channel = get_least_populated_channel()
            name = generate_name()
            
            user = User.create(
                user_id=message.from_user.id,
                name=name,
                channel=channel,
                created_at=datetime.now()
            )
            
            channel_users = User.get_channel_users(channel)
            users_count = len(list(channel_users))
            
            await message.answer(
                f"👋 Привет!\n\n"
                f"📡 Твой канал: `{channel}Hz`\n"
                f"👤 Твоё имя: `{get_display_name(user)}`\n"
                f"👥 Пользователей на канале: `{users_count}`\n\n"
                "📝 Команды:\n"
                "/help - справка",
                parse_mode="Markdown"
            )
        else:
            channel_users = User.get_channel_users(user.channel)
            users_count = len(list(channel_users))
            
            await message.answer(
                f"📡 Твой канал: `{user.channel}Hz`\n"
                f"👤 Твоё имя: `{get_display_name(user)}`\n"
                f"👥 Пользователей на канале: `{users_count}`\n\n"
                "📝 Команды:\n"
                "/help - справка",
                parse_mode="Markdown"
            )
            
    except Exception as e:
        await bot.send_message(
            config.OWNER_ID,
            f"❌ *Error in cmd_start*:\n"
            f"User ID: `{message.from_user.id}`\n"
            f"Error: `{str(e)}`\n"
            f"Traceback: ```\n{traceback.format_exc()}```",
            parse_mode="Markdown"
        )
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_start: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при выполнении команды*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    try:
        current_version = get_current_version()
        base_commands = (
            f"*📡 Версия бота: {current_version}*\n\n"
            "*Доступные команды:*\n\n"
            "• /start - Начать использование бота\n"
            "• /help - Показать это сообщение\n"
            "• /name [имя] - Установить своё имя\n"
            "• /scan - Просканировать каналы\n"
            "• /goto [канал] - Переместиться на канал\n"
            "_Каналы переключаются автоматически_"
        )
        
        owner_commands = (
            "\n\n*👑 Команды владельца:*\n\n"
            "• /version [номер] [текст] - Отправить обновление\n"
            "• /broadcast [текст] - Отправить сообщение всем пользователям\n"
            "• /zov [user_id] [время] [причина] - Отправить в тюрьму\n"
            "• /unzov [user_id] - Освободить из тюрьмы\n"
            "• /emoji [user_id] [emoji] - Установить эмодзи пользователю\n"
            "• /resetname [user_id] - Сбросить кастомное имя\n"
            "• /del - Удалить сообщение (ответом на сообщение)"
        )
        
        help_text = base_commands + (owner_commands if message.from_user.id == config.OWNER_ID else "")
        await message.answer(help_text, parse_mode="Markdown")
        
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_help: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer("Произошла ошибка при отображении помощи.")

@dp.message_handler(commands=['scan'])
async def cmd_scan(message: types.Message):
    try:
        scan_results = []
        
        # Получаем все уникальные каналы из базы данных
        channels = User.select(User.channel).distinct()
        
        for channel_record in channels:
            users = User.get_channel_users(channel_record.channel)
            user_count = len(list(users))
            
            scan_results.append(
                f"👁 Канал: `{channel_record.channel}Hz`\n"
                f"👥 Пользователей: `{user_count}`\n"
            )
        
        if scan_results:
            await message.answer(
                "🔍 *Результаты сканирования:*\n\n" + "\n".join(scan_results),
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                "😔 *Не найдено активных каналов...*",
                parse_mode="Markdown"
            )
            
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_scan: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer("❌ *Ошибка при сканировании*", parse_mode="Markdown")

@dp.message_handler(commands=['goto', 'channel'])
async def cmd_goto(message: types.Message):
    if message.get_command() == '/channel':
        await message.answer(
            "❗️ Ссылка на /goto\n"
            "Пример команды: `/channel канал`, `/goto канал`",
            parse_mode="Markdown"
        )
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer(
                "❌ *Ошибка*: Неверный формат команды\n"
                "Используйте: `/goto канал`",
                parse_mode="Markdown"
            )
            return
            
        channel = int(args[1])
        user_id = message.from_user.id
        
        # Проверка на тюремный канал
        if channel == config.PRISON_CHANNEL:
            await message.answer(
                random.choice(config.PRISON_MESSAGES),
                parse_mode="Markdown"
            )
            return
            
        if channel < config.MIN_CHANNEL or channel > config.MAX_CHANNEL:
            await message.answer(
                "❌ *Ошибка*: Недопустимый канал\n"
                f"Диапазон: от `{config.MIN_CHANNEL}` до `{config.MAX_CHANNEL}`",
                parse_mode="Markdown"
            )
            return
            
        user = User.get_user(user_id)
        if not user:
            await message.answer(
                "❌ *Ошибка*: Пользователь не найден",
                parse_mode="Markdown"
            )
            return

        # Обновляем только канал, имя не трогаем если есть кастомное
        if not user.custom_name:
            user.name = generate_name()
        user.channel = channel
        user.save()
        
        # Сообщение пользователю
        await message.answer(
            "😌 *Ты ненадолго закрыл глаза*...\n"
            f"_И оказался на канале_ `{channel}Hz`",
            parse_mode="Markdown",
            reply_markup=create_user_button(user.name, user)
        )
            
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_goto: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при выполнении команды*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['name', 'tag'])
async def cmd_name(message: types.Message):
    if message.get_command() == '/tag':
        await message.answer(
            "❗️ Ссылка на команду /name\n"
            "Пример команды: `/tag ник`, `/name ник`",
            parse_mode="Markdown"
        )
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.answer(
                "❌ *Ошибка*: Укажите имя\n"
                "Используйте: `/name Новое Имя`",
                parse_mode="Markdown"
            )
            return
            
        new_name = args[1]
        user_id = message.from_user.id
        
        # Проверяем, разрешено ли имя
        is_allowed, error_msg = is_name_allowed(new_name, user_id)
        if not is_allowed:
            await message.answer(error_msg, parse_mode="Markdown")
            return
            
        # Получаем или создаем пользователя
        user = User.get_or_none(User.user_id == user_id)
        if not user:
            await message.answer(
                "❌ *Ошибка*: Сначала используйте /start",
                parse_mode="Markdown"
            )
            return
            
        # Обновляем имя
        user.custom_name = new_name
        user.save()
        
        await message.answer(
            f"✅ Установлено имя: `{new_name}`",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_name: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при выполнении команды*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['resetname'])
@owner_only
async def cmd_resetname(message: types.Message):
    try:
        args = message.text.split()
        user_id = None
        
        # Если это ответ на сообщение
        if message.reply_to_message:
            user_id = message.reply_to_message.from_user.id
        else:
            # Стандартный вариант через ID
            if len(args) != 2:
                await message.answer(
                    "❌ *Ошибка*: Неверный формат команды\n"
                    "Используйте:\n"
                    "• `/resetname user_id`\n"
                    "• Ответом на сообщение: `/resetname`",
                    parse_mode="Markdown"
                )
                return
            user_id = int(args[1])
            
        user = User.get_user(user_id)
        if not user:
            await message.answer(
                "❌ *Ошибка*: Пользователь не найден",
                parse_mode="Markdown"
            )
            return
            
        user.custom_name = None
        user.save()
        
        await message.answer(
            "✅ Кастомное имя удалено",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_resetname: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при выполнении команды*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['emoji'])
@owner_only
async def cmd_emoji(message: types.Message):
    try:
        args = message.text.split()
        user_id = None
        emoji = None
        
        # Если это ответ на сообщение
        if message.reply_to_message:
            user_id = message.reply_to_message.from_user.id
            if len(args) != 2:
                await message.answer(
                    "❌ *Ошибка*: Укажите эмодзи\n"
                    "Используйте: `/emoji 🌟`",
                    parse_mode="Markdown"
                )
                return
            emoji = args[1]
        else:
            # Стандартный вариант через ID
            if len(args) != 3:
                await message.answer(
                    "❌ *Ошибка*: Неверный формат команды\n"
                    "Используйте:\n"
                    "• `/emoji user_id emoji`\n"
                    "• Ответом на сообщение: `/emoji emoji`",
                    parse_mode="Markdown"
                )
                return
            user_id = int(args[1])
            emoji = args[2]
            
        user = User.get_user(user_id)
        if not user:
            await message.answer(
                "❌ *Ошибка*: Пользователь не найден",
                parse_mode="Markdown"
            )
            return
            
        user.emoji = emoji
        user.save()
        
        await message.answer(
            f"✅ Установлен эмодзи: {emoji}",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_emoji: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при выполнении команды*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['version'])
@owner_only
async def cmd_version(message: types.Message):
    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 3:
            await message.answer(
                "❌ *Ошибка*: Неверный формат команды\n"
                "Используйте: `/version номер текст`",
                parse_mode="Markdown"
            )
            return
            
        version = args[1]
        text = args[2]
        
        # Обновляем версию
        with open('version.txt', 'w') as f:
            f.write(version)
            
        # Формируем сообщение об обновлении
        update_message = (
            "🌟 *Обновление!*\n\n"
            f"📦 Версия: `{version}`\n"
            "📝 Что нового:\n"
            f"{text}"
        )
        
        # Создаем кнопку "Система"
        markup = create_user_button("Система")
        
        # Отправляем в канал обновлений
        try:
            await bot.send_message(
                chat_id=config.UPDATE_CHANNEL,
                text=update_message,
                parse_mode="Markdown"
            )
        except Exception as e:
            print(f"Failed to send update to channel: {e}")
        
        # Отправляем всем пользователям
        users = User.select()
        sent_count = 0
        
        for user in users:
            try:
                await send_message(
                    chat_id=user.user_id,
                    text=update_message,
                    parse_mode="Markdown",
                    reply_markup=markup
                )
                if user.user_id != message.from_user.id:  # Не считаем отправителя
                    sent_count += 1
            except Exception as e:
                print(f"Failed to send update to {user.user_id}: {e}")
        
        # Отправляем статистику
        await message.answer(
            f"✅ Обновление отправлено {sent_count} пользователям и в канал",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_version: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при отправке обновления*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['broadcast', 'broad'])
@owner_only
async def cmd_broadcast(message: types.Message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.answer(
                "❌ *Ошибка*: Укажите сообщение\n"
                "Используйте: `/broadcast текст` или `/broad текст`\n\n"
                "Форматирование:\n"
                "`nn` - код\n"
                "*nn* - жирный текст\n"
                "_nn_ - курсив\n"
                "~nn~ - зачеркнутый\n"
                "||nn|| - спойлер\n"
                "@username - упоминание\n"
                "`#ffffff` - цвет (HEX)",
                parse_mode="Markdown"
            )
            return
            
        text = args[1]
        users = User.select()
        sent_count = 0
        markup = create_user_button("Владелец")
        
        # Заменяем тире на обычное
        text = text.replace('—', '-')
        
        for user in users:
            try:
                await bot.send_message(
                    user.user_id,
                    text,
                    parse_mode="Markdown",
                    reply_markup=markup,
                    disable_web_page_preview=False
                )
                sent_count += 1
                # Добавляем задержку между отправками
                await asyncio.sleep(config.BROADCAST_DELAY)
                
            except BotBlocked:
                print(f"Bot was blocked by user {user.user_id}, removing from database")
                user.delete_instance()
            except Exception as e:
                print(f"Failed to send broadcast to {user.user_id}: {e}")
                continue
                
        status_msg = await message.answer(
            f"✅ Сообщение отправлено `{sent_count}` пользователям",
            parse_mode="Markdown"
        )
        asyncio.create_task(delete_message_after(status_msg, config.DELETE_STATS_AFTER))
            
    except Exception as e:
        await bot.send_message(
            config.OWNER_ID,
            f"❌ *Error in cmd_broadcast*:\n"
            f"Error: `{str(e)}`\n"
            f"Traceback: ```\n{traceback.format_exc()}```",
            parse_mode="Markdown"
        )
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_broadcast: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при отправке сообщения*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['zov'])
@owner_only
async def cmd_zov(message: types.Message):
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.answer(
                "❌ *Ошибка*: Укажите user_id\n"
                "Используйте:\n"
                "• `/zov user_id` _(навсегда)_\n"
                "• `/zov user_id 30m` _(на 30 минут)_\n"
                "• `/zov user_id 2h` _(на 2 часа)_\n"
                "• `/zov user_id 1d` _(на 1 день)_\n"
                "• `/zov user_id причина` _(навсегда с причиной)_\n"
                "• `/zov user_id 30m причина` _(на время с причиной)_",
                parse_mode="Markdown"
            )
            return

        user_id = int(args[1])
        until = None
        reason = None
        
        if len(args) > 2:
            # Проверяем, является ли второй аргумент временем
            time_str = args[2].lower()
            if any(time_str.endswith(unit) for unit in ['m', 'h', 'd']):
                try:
                    # Парсим время
                    multiplier = {'m': 60, 'h': 3600, 'd': 86400}
                    time_value = int(time_str[:-1])
                    time_unit = time_str[-1]
                    
                    if time_unit in multiplier:
                        until = int(time.time()) + (time_value * multiplier[time_unit])
                        # Если есть причина после времени
                        if len(args) > 3:
                            reason = ' '.join(args[3:])
                except ValueError:
                    # Если не удалось распарсить время, считаем это причиной
                    reason = ' '.join(args[2:])
            else:
                # Если второй аргумент не время, значит это причина
                reason = ' '.join(args[2:])
        
        user = User.get_user(user_id)
        if not user:
            await message.answer(
                "❌ *Ошибка*: Пользователь не найден",
                parse_mode="Markdown"
            )
            return
        
        # Сохраняем в базу
        PrisonUser.create(
            user_id=user_id,
            reason=reason or "Не указана",
            until=until
        )
        
        # Перемещаем пользователя
        user.channel = config.PRISON_CHANNEL
        user.save()
        
        # Формируем сообщение для владельца
        status = []
        status.append(f"👤 *Пользователь*: `{get_display_name(user)}`")
        
        if until:
            time_left = until - int(time.time())
            hours = time_left // 3600
            minutes = (time_left % 3600) // 60
            status.append(f"⏱ *Время*: `{hours}ч {minutes}м`")
        else:
            status.append("⏱ *Время*: `навсегда`")
            
        if reason:
            status.append(f"📝 *Причина*: `{reason}`")
        
        # Отправляем сообщение пользователю
        await bot.send_message(
            user_id,
            "😵 *Ты потерял сознание*...\n"
            f"_После того, как ты очнулся, ты оказался на канале_ `{config.PRISON_CHANNEL}Hz`...\n"
            "💀 *Ты не можешь говорить*...",
            parse_mode="Markdown"
        )
        
        # Отправляем подтверждение владельцу
        await message.answer(
            f"✅ *Пользователь помещен в тюрьму*\n\n" + 
            '\n'.join(status),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_zov: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при выполнении команды*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['unzov'])
@owner_only
async def cmd_unzov(message: types.Message):
    try:
        args = message.text.split()
        if len(args) != 2:
            await message.answer(
                "❌ *Ошибка*: Неверный формат команды\n"
                "Используйте: `/unzov user_id`",
                parse_mode="Markdown"
            )
            return
            
        user_id = int(args[1])
        prison_user = PrisonUser.get_or_none(PrisonUser.user_id == user_id)
        
        if not prison_user:
            await message.answer(
                "❌ *Ошибка*: Пользователь не находится в тюрьме",
                parse_mode="Markdown"
            )
            return
            
        prison_user.delete_instance()
        
        # Перемещаем пользователя на случайный канал
        user = User.get_user(user_id)
        if user:
            channel = get_random_channel()
            user.channel = channel
            user.save()
            
            await bot.send_message(
                user_id,
                "🌟 *Ты пришел в себя*...\n"
                f"_И оказался на канале_ `{channel}Hz`",
                parse_mode="Markdown"
            )
        
        await message.answer(
            "✅ Пользователь освобожден",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_unzov: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при выполнении команды*",
            parse_mode="Markdown"
        )

@dp.message_handler(commands=['del'])
@owner_only
async def cmd_del(message: types.Message):
    try:
        # Получаем сообщение, на которое ответили
        if not message.reply_to_message:
            await message.answer(
                "❌ *Ошибка*: Ответьте на сообщение, которое хотите удалить",
                parse_mode="Markdown"
            )
            return
            
        # Получаем всех пользователей
        users = User.select()
        deleted_count = 0
        base_message_id = message.reply_to_message.message_id
        
        # Удаляем сообщение у всех пользователей
        for i, user in enumerate(users):
            try:
                msg_id = base_message_id + i  # Увеличиваем ID для каждого следующего пользователя
                await bot.delete_message(user.user_id, msg_id)
                deleted_count += 1
            except Exception as e:
                print(f"Failed to delete message {msg_id} for user {user.user_id}: {e}")
                continue
                    
        # Отправляем подтверждение
        status_msg = await message.answer(
            f"✅ Удалено сообщений: `{deleted_count}`",
            parse_mode="Markdown"
        )
        asyncio.create_task(delete_message_after(status_msg, config.DELETE_STATS_AFTER))
            
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in cmd_del: {e}")
        print(f"Message: {message.text}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при удалении сообщения*",
            parse_mode="Markdown"
        )

@dp.message_handler(content_types=['text'])
async def handle_message(message: types.Message):
    try:
        # Проверяем время последнего сообщения в самом начале
        current_time = time.time()
        last_time = last_message_time.get(message.from_user.id, 0)
        if current_time - last_time < config.MESSAGE_DELAY:
            remaining = round(config.MESSAGE_DELAY - (current_time - last_time))
            await message.answer(
                f"⏳ Подождите еще `{remaining}` сек.",
                parse_mode="Markdown"
            )
            return
            
        # Сразу обновляем время последнего сообщения
        last_message_time[message.from_user.id] = current_time
        
        user = User.get_user(message.from_user.id)
        if not user:
            await message.answer(
                "❌ *Ошибка*: Пользователь не найден\n"
                "Используйте /start для регистрации",
                parse_mode="Markdown"
            )
            return
            
        # Проверяем тюрьму
        prison_user = PrisonUser.get_or_none(PrisonUser.user_id == message.from_user.id)
        if prison_user:
            await message.answer(
                f"🚔 Вы в тюрьме еще `{prison_user.remaining_time}` секунд",
                parse_mode="Markdown"
            )
            # Если в тюрьме, сбрасываем таймер
            last_message_time.pop(message.from_user.id, None)
            return
            
        channel_users = User.get_channel_users(user.channel)
        recipients_count = sum(1 for u in channel_users if u.user_id != message.from_user.id)
            
        if recipients_count == 0:
            status_msg = await message.answer(
                f"📡 На канале `{user.channel}Hz` никого нет...",
                parse_mode="Markdown"
            )
            asyncio.create_task(delete_message_after(status_msg, config.DELETE_STATS_AFTER))
            return

        markup = create_user_button(user.name, user)
        
        text = message.text
        reply_msg = None
        
        if message.reply_to_message:
            quoted_user = User.get_user(message.reply_to_message.from_user.id)
            quoted_name = quoted_user.get_display_name() if quoted_user else message.reply_to_message.from_user.first_name
            quoted_text = message.reply_to_message.text
            
            if "╭─" in quoted_text:
                lines = quoted_text.split("\n")
                for i in range(len(lines)-1, -1, -1):
                    if not lines[i].startswith("╭─") and not lines[i].startswith("╰"):
                        quoted_text = lines[i]
                        break
                        
            text_with_quote = (
                f"╭─ {quoted_name}\n"
                f"╰ {quoted_text}\n"
                f"\n"
                f"{text}"
            )
            reply_msg = message.reply_to_message

        # Отправляем сообщения
        status_msg = await message.answer(
            "📡 *Отправляю...*",
            parse_mode="Markdown"
        )
        
        start_time = time.time()
        tasks = []
        results = []

        # Создаем задачи для отправки
        for channel_user in channel_users:
            if channel_user.user_id == message.from_user.id and reply_msg:
                task = bot.send_message(
                    channel_user.user_id,
                    text,
                    reply_to_message_id=reply_msg.message_id,
                    reply_markup=markup
                )
            else:
                task = bot.send_message(
                    channel_user.user_id,
                    text_with_quote if reply_msg else text,
                    reply_markup=markup
                )
            tasks.append(task)

        # Выполняем все задачи параллельно
        try:
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    print(f"Failed to send message to {channel_users[i].user_id}: {response}")
                else:
                    results.append(f"{channel_users[i].user_id}:{response.message_id}")
        except Exception as e:
            print(f"Error in gather: {e}")

        # Сохраняем результаты
        if results:
            StoredMessage.save_message_with_timestamp(
                message.from_user.id,
                user.get_display_name(),
                results,
                text,
                int(time.time())
            )

        execution_time = int((time.time() - start_time) * 1000)
        time_str = f"{execution_time}ms" if execution_time < 1000 else f"{execution_time/1000:.1f}s"
        
        await status_msg.edit_text(
            f"📡 Твой сигнал доставлен на канал `{user.channel}Hz`\n"
            f"👥 Получателей: `{recipients_count}`\n"
            f"⚡️ Время доставки: `{time_str}`",
            parse_mode="Markdown"
        )
        
        asyncio.create_task(delete_message_after(status_msg, config.DELETE_STATS_AFTER))

        # После отправки сообщений
        channel_mapping = message_mappings.setdefault(user.channel, {})
        message_mapping = channel_mapping.setdefault(message.message_id, {})
        
        for result in results:
            if isinstance(result, types.Message):
                message_mapping[result.chat.id] = result.message_id

    except Exception as e:
        print(f"[ERROR] Error in handle_message: {e}")
        traceback.print_exc()
        try:
            await message.answer(
                f"🚫 *Ошибка*: {str(e)}",
                parse_mode="Markdown"
            )
        except:
            pass

async def delete_message_after(message: types.Message, delay: int):
    """Удаляет сообщение после задержки"""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception as e:
        print(f"Failed to delete message: {e}")

@dp.callback_query_handler()
async def process_callback(callback_query: types.CallbackQuery):
    try:
        # Получаем данные из callback_data
        callback_data = callback_query.data
        
        if callback_data == "name":
            # Кнопка с именем пользователя
            await callback_query.answer(
                "Это имя пользователя",
                show_alert=True
            )
            
        elif callback_data == "system":
            # Системное сообщение
            await callback_query.answer(
                "Это системное сообщение",
                show_alert=True
            )
            
        elif callback_data == "owner":
            # Сообщение от владельца
            await callback_query.answer(
                "Это рассылка от владельца",
                show_alert=True
            )
            
    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in process_callback: {e}")
        print(f"Callback data: {callback_query.data}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")

def get_least_populated_channel() -> int:
    """
    Возвращает канал с наименьшим количеством пользователей
    или случайный новый канал
    """
    try:
        # Получаем все каналы и количество пользователей в них
        channels = {}
        for user in User.select():
            channels[user.channel] = channels.get(user.channel, 0) + 1
            
        # Если есть каналы с пользователями
        if channels:
            # Находим канал с минимальным количеством пользователей
            min_channel = min(channels.items(), key=lambda x: x[1])[0]
            # С вероятностью CHANNEL_CREATION_CHANCE создаем новый канал
            if random.random() < config.CHANNEL_CREATION_CHANCE:
                return random.randint(config.MIN_CHANNEL, config.MAX_CHANNEL)
            return min_channel
            
        # Если нет каналов, создаем случайный
        return random.randint(config.MIN_CHANNEL, config.MAX_CHANNEL)
        
    except Exception as e:
        print(f"Error in get_least_populated_channel: {e}")
        # В случае ошибки возвращаем случайный канал
        return random.randint(config.MIN_CHANNEL, config.MAX_CHANNEL)

@dp.message_handler(content_types=['photo', 'video', 'animation', 'document', 'media_group', 'sticker'])
async def handle_media(message: types.Message):
    try:
        # Убираем проверку на MEDIA_ALLOWED_USERS
        start_time = time.time()
        user = User.get_user(message.from_user.id)
        if not user:
            await message.answer(
                "❌ *Ошибка*: Пользователь не найден\n"
                "Используйте /start для регистрации",
                parse_mode="Markdown"
            )
            return
            
        # Проверяем задержку между сообщениями
        current_time = time.time()
        last_time = last_message_time.get(message.from_user.id, 0)
        if current_time - last_time < config.MESSAGE_DELAY:
            await message.answer(
                f"⏳ Подождите еще `{int(config.MESSAGE_DELAY - (current_time - last_time))}` секунд",
                parse_mode="Markdown"
            )
            return

        # Сразу обновляем время последнего сообщения
        last_message_time[message.from_user.id] = current_time

        channel_users = User.get_channel_users(user.channel)
        recipients_count = sum(1 for u in channel_users if u.user_id != message.from_user.id)
            
        if recipients_count == 0:
            status_msg = await message.answer(
                f"📡 На канале `{user.channel}Hz` никого нет...",
                parse_mode="Markdown"
            )
            asyncio.create_task(delete_message_after(status_msg, config.DELETE_STATS_AFTER))
            return

        # Сначала отправляем статус
        status_msg = await message.answer(
            f"📡 Отправка сигнала на канал `{user.channel}Hz`\n"
            f"👥 Получателей: `{recipients_count}`",
            parse_mode="Markdown"
        )

        markup = create_user_button(user.name, user)
        
        # Получаем медиа и подпись
        file_id = None
        caption = message.caption or ""
        media_type = None
        
        # Определяем тип медиа и file_id
        if message.sticker:
            file_id = message.sticker.file_id
            media_type = 'sticker'
        elif message.media_group_id:
            if message.photo:
                file_id = message.photo[-1].file_id
                media_type = 'photo'
            elif message.video:
                file_id = message.video.file_id
                media_type = 'video'
            elif message.document:
                file_id = message.document.file_id
                media_type = 'document'
        else:
            if message.photo:
                file_id = message.photo[-1].file_id
                media_type = 'photo'
            elif message.video:
                file_id = message.video.file_id
                media_type = 'video'
            elif message.animation:
                file_id = message.animation.file_id
                media_type = 'animation'
            elif message.document:
                file_id = message.document.file_id
                media_type = 'document'
            
        if not file_id:
            return

        # Создаем список задач для отправки
        tasks = []
        for channel_user in channel_users:
            async def send_with_delay(user_id):
                await asyncio.sleep(config.BROADCAST_DELAY)
                if media_type == 'sticker':
                    return await bot.send_sticker(user_id, file_id, reply_markup=markup)
                elif media_type == 'photo':
                    return await bot.send_photo(user_id, file_id, caption=caption, reply_markup=markup)
                elif media_type == 'video':
                    return await bot.send_video(user_id, file_id, caption=caption, reply_markup=markup)
                elif media_type == 'animation':
                    return await bot.send_animation(user_id, file_id, caption=caption, reply_markup=markup)
                elif media_type == 'document':
                    return await bot.send_document(user_id, file_id, caption=caption, reply_markup=markup)

            tasks.append(send_with_delay(channel_user.user_id))

        # Отправляем всем параллельно
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Считаем успешные отправки
        successful_sends = sum(1 for r in results if not isinstance(r, Exception))
        
        execution_time = int((time.time() - start_time) * 1000)
        time_str = f"{execution_time}ms" if execution_time < 1000 else f"{execution_time/1000:.1f}s"

        # Обновляем статус
        await status_msg.edit_text(
            f"📡 Сигнал доставлен на канал `{user.channel}Hz`\n"
            f"👥 Получателей: `{successful_sends}/{recipients_count}`\n"
            f"⚡️ Время доставки: `{time_str}`",
            parse_mode="Markdown"
        )
        
        asyncio.create_task(delete_message_after(status_msg, config.DELETE_STATS_AFTER))

    except Exception as e:
        print(f"\n{'='*50}")
        print(f"[ERROR] Error in handle_media: {e}")
        print(traceback.format_exc())
        print(f"{'='*50}\n")
        await message.answer(
            "❌ *Ошибка при отправке медиа*",
            parse_mode="Markdown"
        )

@dp.message_handler(content_types=['message_reaction'])
async def handle_reaction(message: types.Message):
    try:
        # Получаем информацию о реакции
        reaction = message.message_reaction
        if not reaction:
            return
            
        # Получаем канал пользователя
        user = User.get_user(message.from_user.id)
        if not user:
            return
            
        # Получаем mapping сообщений для этого канала
        channel_mapping = message_mappings.get(user.channel, {})
        if not channel_mapping:
            return
            
        # Получаем mapping для конкретного сообщения
        message_mapping = channel_mapping.get(message.message_id, {})
        if not message_mapping:
            return
            
        # Устанавливаем реакцию на все связанные сообщения
        for user_id, msg_id in message_mapping.items():
            try:
                await bot.set_message_reaction(
                    chat_id=user_id,
                    message_id=msg_id,
                    reaction=[types.ReactionType(type="emoji", emoji=reaction.emoji)]
                )
            except Exception as e:
                print(f"Failed to set reaction for user {user_id}: {e}")
                continue
                
        # Устанавливаем реакцию на оригинальное сообщение
        try:
            await bot.set_message_reaction(
                chat_id=message.chat.id,
                message_id=message.message_id,
                reaction=[types.ReactionType(type="emoji", emoji=reaction.emoji)]
            )
        except Exception as e:
            print(f"Failed to set reaction on original message: {e}")

    except Exception as e:
        print(f"[ERROR] Error in handle_reaction: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    init_db()
    init_messages_db()
    asyncio.get_event_loop().create_task(start_channel_switchers())
    executor.start_polling(dp, skip_updates=True)