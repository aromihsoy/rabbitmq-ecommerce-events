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
```
                    ┌──────────────────────────────────────┐
                    │   POST /orders/{id}/pay (order-web)   │
                    └─────────┬────────────────────────────┘
                              │ (одна транзакция БД)
                              ├── UPDATE orders SET status=paid
                              └── INSERT INTO outbox
                                      │
                                      ▼
                              order-relay (поллит outbox)
                                       │ publish
                                       ▼
                          topic exchange "orders"
                                       │
                    ┌──────────────────┴───────────────────┐
                    │ order.paid                order.paid │
                    ▼                                      ▼
             queue "email"                        queue "inventory"
                    │                                      │
                    ▼                                      ▼
          notification-service               inventory-service
          (manual ack, идемпотентный)        (manual ack, идемпотентный)
                    │                                      │
              при падении                          при падении
                    ▼                                      ▼
          email.retry (TTL 5с)               inventory.retry (TTL 5с)
             └─ обратно в очередь               └─ обратно в очередь
                    │                                      │
            после 3 попыток                       после 3 попыток
                    ▼                                      ▼
             email.dlq                          inventory.dlq
```

## Структура проекта
```
├── shared/               # общий код: config, db, models, dedup, retry
├── order_service/        # FastAPI producer + relay
├── notification_service/ # консьюмер email
├── inventory_service/    # консьюмер inventory
├── alembic/              # миграции (общая БД)
└── docker-compose.yml
```

## Что реализовано
- topic exchange + ручная топология
- fan-out
- manual ack, идемпотентность консьюмеров (дедупликация по message_id)
- DLQ для ядовитых сообщений
- prefetch_count
- transactional outbox — событие пишется в БД атомарно со сменой
  статуса заказа, отдельный relay-процесс публикует его в RabbitMQ
  (гарантия, что событие не потеряется, если брокер недоступен)
- retry с backoff (x-death + TTL retry-очереди, потолок попыток -> DLQ)
- распад на сервисы (order/notification/inventory + shared)
- докеризация (Docker compose, healthcheck, миграция отдельным сервисом)

## Запуск
1. cp .env.example .env
2. docker compose up -d
   (migrate накатит схему автоматом, сервисы ждут healthcheck кролика и БД)
3. Swagger: http://localhost:8000/docs
   RabbitMQ management: http://localhost:15672 (guest/guest)

## Тесты
Тесты прогоняются локально (не в контейнере), поэтому вам нужен установленный pip install -r requirements.txt в venv — иначе клонировавший запустит pytest без зависимостей и упадёт.
1. docker compose exec postgres psql -U ecommerce -c "CREATE DATABASE ecommerce_test;"
2. pytest

## Пример
- POST /orders {"amount": 500} → создать заказ
- POST /orders/{id}/pay → оплатить, событие уходит в fan-out
- Событие после оплаты видно в логах: docker compose logs notification

## Известные ограничения
- Один relay-процесс. При запуске нескольких relay одновременно
  возможна повторная публикация события (оба прочитают одни и те же
  неопубликованные строки). Дубли гасятся идемпотентностью консьюмеров.
  Решается блокировкой строк (SELECT ... FOR UPDATE SKIP LOCKED).
- Сервисы разделены по контейнерам, но используют общую БД — не полная изоляция данных, как в каноничных микросервисах.