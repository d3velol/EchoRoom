from peewee import *
import json
import time

messages_db = SqliteDatabase('messages.db')

class BaseModel(Model):
    class Meta:
        database = messages_db

class StoredMessage(BaseModel):
    sender_id = BigIntegerField()
    sender_name = TextField()  # Добавляем имя отправителя
    message_data = TextField()  # Строка с парами user_id:message_id
    message = TextField()
    timestamp = BigIntegerField()
    
    @classmethod
    def save_message_with_timestamp(cls, sender_id, sender_name, results, message, timestamp):
        return cls.create(
            sender_id=sender_id,
            sender_name=sender_name,
            message_data=" ".join(results),
            message=message,
            timestamp=timestamp
        )
        
    @classmethod
    def find_message_details(cls, sender_id, message_id):
        """Ищем сообщение по ID отправителя и ID сообщения"""
        try:
            message = cls.select().where(
                cls.message_data.contains(f"{sender_id}:{message_id}")
            ).first()
            
            if message:
                # Разбираем message_data на пары user_id:message_id
                pairs = message.message_data.split()
                recipient_messages = []
                for pair in pairs:
                    user_id, msg_id = pair.split(':')
                    recipient_messages.append((int(user_id), int(msg_id)))
                    
                return message.sender_id, message_id, message.message, message.timestamp, recipient_messages
                
        except Exception as e:
            print(f"Error finding message: {e}")
            
        return None

def init_messages_db():
    messages_db.connect()
    
    # Проверяем существование таблицы и столбца sender_name
    cursor = messages_db.execute_sql('PRAGMA table_info(storedmessage)')
    columns = [column[1] for column in cursor.fetchall()]
    
    if 'sender_name' not in columns:
        # Если нет столбца sender_name, пересоздаем таблицу
        messages_db.drop_tables([StoredMessage], safe=True)
        messages_db.create_tables([StoredMessage], safe=True)
        print("Messages database schema updated successfully")
    else:
        messages_db.create_tables([StoredMessage], safe=True)