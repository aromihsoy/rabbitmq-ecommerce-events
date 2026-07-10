from unittest.mock import AsyncMock
from sqlalchemy import select
from order_service.relay import publish_pending
from shared.models import Outbox




async def test_publish_penging_publishes_and_marks(session):
    session.add(Outbox(
        message_id="msg-1",
        routing_key="order.paid",
        payload="order 1 paid",
    ))
    await session.commit()

    exchange = AsyncMock()
    count = await publish_pending(session, exchange)

    assert count == 1
    exchange.publish.assert_awaited_once()

    result = await session.execute(select(Outbox))
    row = result.scalars().one()
    assert row.published_at is not None


async def test_publish_pending_skips_published(session):
    from datetime import datetime, timezone
    session.add(Outbox(
        message_id="msg-2",
        routing_key="order.paid",
        payload="order 2 paid",
        published_at=datetime.now(timezone.utc),
    ))
    await session.commit()

    exchange = AsyncMock()
    count = await publish_pending(session, exchange)

    assert count == 0
    exchange.publish.assert_not_awaited()