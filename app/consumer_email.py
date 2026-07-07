import asyncio
import logging


import aio_pika


from app.config import settings
from app.db import session_maker
from app.dedup import mark_processed




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

        email_queue = await channel.declare_queue(
            "email",
            arguments={
                "x-dead-letter-exchange": "orders.dlx",
                "x-dead-letter-routing-key": "email.dead",
            },
        )
        await email_queue.bind(exchange, routing_key="order.paid")

        async with email_queue.iterator() as queue_iter:
            async for message in queue_iter:
                try:
                    async with message.process(requeue=False):
                        mid = message.message_id
                        if mid is None:
                            raise ValueError("message without message_id")

                        async with session_maker() as session:
                            async with session.begin():
                                is_new = await mark_processed(session, "email", mid)

                                if is_new:
                                    if "poison" in message.body.decode():
                                        raise ValueError("ядовитое сообщение")
                                    print(f"NEW {mid} - отправляю письмо")
                                else:
                                    print(f"DUP {mid} - уже было обработано, пропуск")

                except Exception as e:
                    print(f"REJECTED {message.message_id} > DLQ: {e}")

if __name__ == "__main__":
    asyncio.run(main())

