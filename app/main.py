from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager
import aio_pika, uuid

from app.config import settings
from app.db import engine, session_maker
from app.models import Order




@asynccontextmanager
async def lifespan(app: FastAPI):
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()
    exchange = await channel.declare_exchange("orders", aio_pika.ExchangeType.TOPIC)

    app.state.exchange = exchange

    yield

    await connection.close()
    await engine.dispose()



app = FastAPI(title="E-commerce event", lifespan=lifespan)



@app.post("/orders/{order_id}/pay")
async def pay_order(order_id: int, request: Request):
    async with session_maker() as session:
        async with session.begin():
            order = await session.get(Order, order_id)

            if order is None:
                raise HTTPException(404, "Order not found")
            if order.status == "paid":
                raise HTTPException(409, "Order already paid")
            
            order.status = "paid"

    message_id = str(uuid.uuid4())
    await request.app.state.exchange.publish(
        aio_pika.Message(body=f"order {order_id} paid".encode(), message_id=message_id),
        routing_key="order.paid",
    )
