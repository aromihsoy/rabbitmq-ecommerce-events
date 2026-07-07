import pytest_asyncio
from unittest.mock import AsyncMock
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.db import get_session




@pytest_asyncio.fixture
async def client(session):
    async def override_get_session():
        yield session
    app.dependency_overrides[get_session] = override_get_session

    app.state.exchange = AsyncMock()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    
    app.dependency_overrides.clear()


async def test_create_order(client):
    resp = await client.post("/orders", json={"amount": 500})
    assert resp.status_code == 201
    assert resp.json()["status"] == "pending"


async def test_pay_order(client):
    created = await client.post("/orders", json={"amount": 500})
    order_id = created.json()["id"]

    resp = await client.post(f"/orders/{order_id}/pay")
    assert resp.status_code == 200
    assert resp.json()["status"] == "paid"
    app.state.exchange.publish.assert_awaited_once()


async def test_pay_already_paid(client):
    created = await client.post("/orders", json={"amount": 500})
    order_id = created.json()["id"]

    await client.post(f"/orders/{order_id}/pay")
    resp = await client.post(f"/orders/{order_id}/pay")
    assert resp.status_code == 409