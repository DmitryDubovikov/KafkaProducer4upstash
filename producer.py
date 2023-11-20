import json
import random
import time
from datetime import datetime

from dotenv import dotenv_values
from faker import Faker
from kafka import KafkaProducer

config = dotenv_values(".env")

# объявление продюсера Kafka
producer = KafkaProducer(
    bootstrap_servers=[config["BOOTSTRAP_SERVERS"]],
    sasl_mechanism="SCRAM-SHA-256",
    security_protocol="SASL_SSL",
    sasl_plain_username=config["SASL_PLAIN_USERNAME"],
    sasl_plain_password=config["SASL_PLAIN_PASSWORD"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic = "MyTopic"  # название топика

fake = Faker()

# списки полей в заявке
products = [
    "bred",
    "garlic",
    "oil",
    "apples",
    "water",
    "soup",
    "dress",
    "tea",
    "cacao",
    "coffee",
    "bananas",
    "butter",
    "eggs",
    "oatmeal",
]
questions = ["payment", "delivery", "discount", "vip", "staff"]

# бесконечный цикл публикации данных
try:
    while True:
        # подготовка данных для публикации в JSON-формате
        now = datetime.now()
        id = now.strftime("%m/%d/%Y %H:%M:%S")

        content = ""
        theme = ""
        corp = 0
        part = 0

        # подготовка списка возможных ключей маршрутизации (routing keys)
        corp = random.choice([1, 0])

        if corp == 1:
            name = fake.company()
        else:
            name = fake.name()

        # случайный выбор предмета обращения
        subject = random.choice(["app", "question"])

        # добавление дополнительных данных для заголовка и тела сообщения в зависимости от темы заявки
        if subject == "question":
            theme = random.choice(questions)
            content = "Hello, I have a question about " + theme
            part = 0  # все вопросы записывать в раздел 0
        else:
            theme = "app"
            content = random.choice(products) + " " + str(random.randint(0, 100))
            if corp == 1:
                part = 1  # все корпоративные заявки записывать в раздел 1
            else:
                part = 2  # заявки от частных лиц записывать в раздел 2

        # создаем полезную нагрузку в JSON
        data = {"id": id, "name": name, "subject": subject, "content": content}

        # публикуем данные в Kafka
        future = producer.send(topic, value=data, partition=part)
        record_metadata = future.get(timeout=60)
        print(f" [x] Sent {record_metadata}")
        print(f" [x] Payload {data}")

        # повтор через 3 секунды
        time.sleep(3)

except KeyboardInterrupt:
    print("Прервано с клавиатуры")

# Закрываем соединения
producer.close()
