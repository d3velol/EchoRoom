from peewee import *
from datetime import datetime
import json

db = SqliteDatabase('users.db')

class BaseModel(Model):
    class Meta:
        database = db

class User(BaseModel):
    user_id = BigIntegerField(unique=True)
    channel = IntegerField()
    name = TextField()  # Базовое имя
    custom_name = TextField(null=True)  # Кастомное имя
    emoji = TextField(null=True)  # Эмодзи
    created_at = DateTimeField(default=datetime.now)
    
    @classmethod
    def create_or_update(cls, **kwargs):
        user = cls.get_or_none(cls.user_id == kwargs['user_id'])
        if user:
            for key, value in kwargs.items():
                setattr(user, key, value)
            user.save()
            return user
        return cls.create(**kwargs)
    
    @classmethod
    def get_user(cls, user_id):
        return cls.get_or_none(cls.user_id == user_id)
    
    @classmethod
    def get_channel_users(cls, channel):
        return cls.select().where(cls.channel == channel)
    
    def get_display_name(self):
        """Получить отображаемое имя пользователя"""
        display_name = self.custom_name if self.custom_name else self.name
        if self.emoji:
            display_name = f"{self.emoji} {display_name}"
        return display_name

class PrisonUser(BaseModel):
    user_id = BigIntegerField(unique=True)
    reason = TextField()
    until = BigIntegerField(null=True)  # null значит навсегда
    created_at = DateTimeField(default=datetime.now)
    
    @property
    def remaining_time(self):
        """Возвращает оставшееся время в секундах"""
        if self.until is None:  # Если срок вечный
            return float('inf')
        remaining = self.until - int(datetime.now().timestamp())
        return max(0, remaining)  # Не возвращаем отрицательное время

def init_db():
    db.connect()
    
    # Создаем таблицы если их нет
    db.create_tables([User, PrisonUser], safe=True)
    
    # Проверяем существование новых колонок
    cursor = db.execute_sql('PRAGMA table_info(user)')
    columns = [column[1] for column in cursor.fetchall()]
    
    # Добавляем новые колонки если их нет
    if 'emoji' not in columns:
        db.execute_sql('ALTER TABLE user ADD COLUMN emoji TEXT NULL')
    if 'custom_name' not in columns:
        db.execute_sql('ALTER TABLE user ADD COLUMN custom_name TEXT NULL')
        
    # Удаляем старые ненужные колонки если они есть
    if 'use_custom_name' in columns:
        # Копируем данные во временную таблицу
        db.execute_sql('''
            CREATE TABLE user_backup(
                id INTEGER PRIMARY KEY,
                user_id BIGINT UNIQUE,
                channel INTEGER,
                name TEXT,
                custom_name TEXT,
                emoji TEXT,
                created_at DATETIME
            )
        ''')
        db.execute_sql('''
            INSERT INTO user_backup 
            SELECT id, user_id, channel, name, custom_name, emoji, created_at 
            FROM user
        ''')
        # Удаляем старую таблицу
        db.execute_sql('DROP TABLE user')
        # Создаем новую таблицу
        db.execute_sql('''
            CREATE TABLE user(
                id INTEGER PRIMARY KEY,
                user_id BIGINT UNIQUE,
                channel INTEGER,
                name TEXT,
                custom_name TEXT,
                emoji TEXT,
                created_at DATETIME
            )
        ''')
        # Восстанавливаем данные
        db.execute_sql('''
            INSERT INTO user 
            SELECT id, user_id, channel, name, custom_name, emoji, created_at 
            FROM user_backup
        ''')
        # Удаляем временную таблицу
        db.execute_sql('DROP TABLE user_backup')
    
    print("Database updated successfully")