# rabbitmq-ecommerce-events

Event-driven backend для e-commerce: одно событие «заказ оплачен»
веером уходит нескольким независимым консьюмерам с разной логикой
(отправка письма, списание товара) через RabbitMQ.

Проект написан, чтобы показать AMQP-модель по-настоящему — ручную
топологию, маршрутизацию и надёжную доставку. То, чего не умеет
Redis Pub/Sub: fan-out одного события нескольким получателям,
сообщения не теряются, упавший консьюмер переобрабатывает сообщение.

## Стек
FastAPI, PostgreSQL + async SQLAlchemy, RabbitMQ (aio-pika),
Alembic, Docker Compose

## Топология
                    ┌─────────────────────────────────────┐
                    │   POST /orders/{id}/pay (FastAPI)    │
                    └──────────────────┬──────────────────┘
                                       │ publish (order.paid)
                                       ▼
                          topic exchange "orders"
                                       │
                    ┌──────────────────┴──────────────────┐
                    │ order.paid                order.paid │
                    ▼                                      ▼
             queue "email"                        queue "inventory"
                    │                                      │
                    ▼                                      ▼
          consumer (manual ack,               consumer (manual ack,
           идемпотентный)                       идемпотентный)
                    │                                      │
       (ядовитое сообщение)                   (ядовитое сообщение)
                    │                                      │
                    ▼                                      ▼
        orders.dlx → email.dlq            orders.dlx → inventory.dlq

## Что реализовано
- fan-out через topic exchange
- manual ack, идемпотентность консьюмеров (дедупликация по message_id)
- DLQ для ядовитых сообщений
- prefetch_count

## Запуск
1. docker compose up -d
2. дождаться старта контейнеров. Иначе alembic упадёт на connect.
3. alembic upgrade head
4. uvicorn app.main:app --reload
5. консьюмеры: python -m app.consumer_email / consumer_inventory

## Пример
POST /orders {"amount": 500} → создать заказ
POST /orders/{id}/pay → оплатить, событие уходит в fan-out

## Не реализовано / планы
retry с backoff, transactional outbox, распад на сервисы
