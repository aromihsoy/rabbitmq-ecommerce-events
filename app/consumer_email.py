import asyncio
import logging

from sqlalchemy.dialects.postgresql import insert

import aio_pika


from app.config import settings
from app.models import ProcessedMessage
from app.db import session_maker




async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(
        settings.rabbitmq_url
    )


    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("orders", aio_pika.ExchangeType.TOPIC)
        queue = await channel.declare_queue("email")
        await queue.bind(exchange, routing_key="order.paid")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    mid = message.message_id

                    async with session_maker() as session:
                        async with session.begin():
                            stmt = (
                                insert(ProcessedMessage)
                                .values(consumer="email", message_id=mid)
                                .on_conflict_do_nothing()
                                .returning(ProcessedMessage.message_id)
                            )
                            result = await session.execute(stmt)
                            is_new = result.scalar_one_or_none() is not None

                            if is_new:
                                print(f"NEW {mid} - отправляю письмо")
                            else:
                                print(f"DUP {mid} - уже было обработано, пропуск")


if __name__ == "__main__":
    asyncio.run(main())

