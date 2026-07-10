import asyncio
import logging


import aio_pika


from shared.config import settings
from shared.db import session_maker
from shared.dedup import mark_processed
from shared.retry import get_death_count, MAX_RETRIES




async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(
        settings.rabbitmq_url
    )


    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        exchange = await channel.declare_exchange("orders", aio_pika.ExchangeType.TOPIC)
        dlx = await channel.declare_exchange("orders.dlx", aio_pika.ExchangeType.DIRECT)

        email_dlq = await channel.declare_queue("email.dlq")
        await email_dlq.bind(dlx, routing_key="email.dead")

        email_retry = await channel.declare_queue(
            "email.retry",
            arguments={
                "x-message-ttl": 5000,
                "x-dead-letter-exchange": "orders.dlx",
                "x-dead-letter-routing-key": "email.work",
            },
        )
        await email_retry.bind(dlx, routing_key="email.retry")

        email_queue = await channel.declare_queue(
            "email",
            arguments={
                "x-dead-letter-exchange": "orders.dlx",
                "x-dead-letter-routing-key": "email.retry",
            },
        )
        await email_queue.bind(exchange, routing_key="order.paid")
        await email_queue.bind(dlx, routing_key="email.work")

        async with email_queue.iterator() as queue_iter:
            async for message in queue_iter:
                count = get_death_count(message)

                if count >= MAX_RETRIES:
                    await exchange.publish(
                        aio_pika.Message(
                            body=message.body,
                            message_id=message.message_id,
                        ),
                        routing_key="email.dead",
                    )
                    await message.ack()
                    print(f"MAX RETRIES {message.message_id} -> DLQ")
                    continue

                try:
                    mid = message.message_id
                    if mid is None:
                        raise ValueError("message without message_id")

                    async with session_maker() as session:
                        async with session.begin():
                            is_new = await mark_processed(session, "email", mid)

                            if is_new:
                                # демо-триггер для проверки retry/DLQ (проверял с помощью producer'а)
                                if "poison" in message.body.decode():
                                    raise ValueError("ядовитое сообщение")
                                print(f"NEW {mid} - отправляю письмо")
                            else:
                                print(f"DUP {mid} - уже было обработано, пропуск")
                    await message.ack()

                except Exception as e:
                    await message.reject(requeue=False)
                    print(f"RETRY {message.message_id} (attempt {count}) -> {e}")

if __name__ == "__main__":
    asyncio.run(main())

