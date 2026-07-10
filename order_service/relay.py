import asyncio
from datetime import datetime, timezone

import aio_pika
from sqlalchemy import select


from shared.config import settings
from shared.db import session_maker
from shared.models import Outbox


async def publish_pending(session, exchange) -> int:
    result = await session.execute(
        select(Outbox)
        .where(Outbox.published_at.is_(None))
        .order_by(Outbox.created_at)
    )

    rows = result.scalars().all()

    for row in rows:
        await exchange.publish(
            aio_pika.Message(
                body=row.payload.encode(),
                message_id=row.message_id,
            ),
            routing_key=row.routing_key,
        )
        row.published_at = datetime.now(timezone.utc)
    
    return len(rows)


async def main() -> None:
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)

    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("orders", aio_pika.ExchangeType.TOPIC)

        while True:
            async with session_maker() as session:
                async with session.begin():
                    await publish_pending(session, exchange)
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())