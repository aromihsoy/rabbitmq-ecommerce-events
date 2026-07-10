from sqlalchemy.dialects.postgresql import insert

from shared.models import ProcessedMessage




async def mark_processed(session, consumer: str, message_id: str) -> bool:
    stmt = (
        insert(ProcessedMessage)
        .values(consumer=consumer, message_id=message_id)
        .on_conflict_do_nothing()
        .returning(ProcessedMessage.message_id)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None