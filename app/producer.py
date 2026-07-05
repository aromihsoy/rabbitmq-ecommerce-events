import asyncio
import uuid

import aio_pika

from app.config import settings




async def main() -> None:
    connection = await aio_pika.connect_robust(
        settings.rabbitmq_url
    )

    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("orders", aio_pika.ExchangeType.TOPIC)
        message_id = str(uuid.uuid4())
        await exchange.publish(
            aio_pika.Message(body=b"order paid", message_id=message_id),
            routing_key="order.paid",
        )

if __name__ == "__main__":
    asyncio.run(main())