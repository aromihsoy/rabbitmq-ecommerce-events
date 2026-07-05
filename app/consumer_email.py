import asyncio
import logging

import aio_pika

from app.config import settings




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
                    print(message.body)


if __name__ == "__main__":
    asyncio.run(main())

