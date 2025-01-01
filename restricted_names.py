# Словарь для замены похожих символов на стандартные латинские
CHAR_MAP = {
    # Кириллица -> Латиница
    'а': 'a',
    'в': 'b',
    'с': 'c',
    'е': 'e',
    'ё': 'e',
    'і': 'i',
    'к': 'k',
    'м': 'm',
    'н': 'n',
    'о': 'o',
    'р': 'p',
    'ѕ': 's',
    'т': 't',
    'у': 'y',
    'х': 'x',
    
    # Специальные символы и цифры
    '4': 'a',
    '3': 'e',
    '1': 'i',
    '0': 'o',
    '@': 'a',
    '$': 's',
    '7': 't',
    
    # Другие похожие символы Unicode
    'ᴀ': 'a',
    'ʙ': 'b',
    'ᴄ': 'c',
    'ᴅ': 'd',
    'ᴇ': 'e',
    'ɢ': 'g',
    'ʜ': 'h',
    'ɪ': 'i',
    'ᴊ': 'j',
    'ᴋ': 'k',
    'ʟ': 'l',
    'ᴍ': 'm',
    'ɴ': 'n',
    'ᴏ': 'o',
    'ᴘ': 'p',
    'ǫ': 'q',
    'ʀ': 'r',
    'ѕ': 's',
    'ᴛ': 't',
    'ᴜ': 'u',
    'ᴠ': 'v',
    'ᴡ': 'w',
    'х': 'x',
    'ʏ': 'y',
    'ᴢ': 'z'
}

# Список запрещенных имен
RESTRICTED_NAMES = {
    "Система": [],  # Запрещено всем
    "Владелец": [8019871856],  # Разрешено только владельцу
    "Admin": [8019871856],
    "Administrator": [8019871856],
    "Администратор": [8019871856],
    "System": [],
    "Bot": [],
    "Бот": [],
    "Owner": [8019871856],
    "Support": [],
    "Поддержка": [],
    "Модератор": [],
    "Moderator": [],
    "СИСТЕМНОЕ СООБЩЕНИЕ": [],
    "#SYSTEM_MESSAGE": []
}

def normalize_text(text: str) -> str:
    """
    Нормализует текст, заменяя все похожие символы на стандартные латинские
    и приводя к нижнему регистру
    """
    text = text.lower()
    normalized = ""
    for char in text:
        normalized += CHAR_MAP.get(char, char)
    return normalized

def is_valid_name(name: str) -> tuple[bool, str]:
    """
    Проверяет валидность имени
    
    Args:
        name: Имя для проверки
    
    Returns:
        tuple[bool, str]: (Валидно ли имя, Сообщение об ошибке)
    """
    # Проверка длины
    if len(name) > 16:
        return False, "❌ *Ошибка*: Максимальная длина имени - 16 символов"
    
    # Проверка символов
    allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZабвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ ')
    invalid_chars = [char for char in name if char not in allowed_chars]
    
    if invalid_chars:
        return False, f"❌ *Ошибка*: Запрещенные символы в имени: `{'`, `'.join(set(invalid_chars))}`"
    
    # Проверяем, что имя не состоит только из пробелов
    if name.strip() == '':
        return False, "❌ *Ошибка*: Имя не может состоять только из пробелов"
    
    return True, ""

def is_name_allowed(name: str, user_id: int) -> tuple[bool, str]:
    """
    Проверяет, разрешено ли использование имени пользователю
    
    Args:
        name: Имя для проверки
        user_id: ID пользователя
    
    Returns:
        tuple[bool, str]: (Разрешено ли имя, Сообщение об ошибке)
    """
    # Проверка длины
    if len(name) > 32:  # Увеличено до 32 символов
        return False, "❌ *Ошибка*: Максимальная длина имени - 32 символа"
    
    # Проверка символов
    allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZабвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ 0123456789')  # Добавлены цифры
    invalid_chars = [char for char in name if char not in allowed_chars]
    
    if invalid_chars:
        return False, f"❌ *Ошибка*: Запрещенные символы в имени: `{'`, `'.join(set(invalid_chars))}`"
    
    # Проверяем, что имя не состоит только из пробелов
    if name.strip() == '':
        return False, "❌ *Ошибка*: Имя не может состоять только из пробелов"
    
    # Проверяем запрещенные имена
    normalized_name = normalize_text(name)
    
    # Проверяем только точное совпадение с запрещенными именами
    for restricted_name, allowed_users in RESTRICTED_NAMES.items():
        if normalize_text(restricted_name) == normalized_name:
            if not allowed_users or user_id not in allowed_users:
                return False, "❌ *Ошибка*: Это имя запрещено к использованию"
                
    return True, "" 