from shared.dedup import mark_processed




async def test_first_insert_is_new(session):
    is_new = await mark_processed(session, "email", "msg-1")
    assert is_new is True


async def test_duplicate_is_not_new(session):
    await mark_processed(session, "email", "msg-1")
    is_new = await mark_processed(session, "email", "msg-1")
    assert is_new is False


async def test_same_message_different_consumers(session):
    email_new = await mark_processed(session, "email", "msg-1")
    inventory_new = await mark_processed(session, "inventory", "msg-1")
    assert email_new is True
    assert inventory_new is True