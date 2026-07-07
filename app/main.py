from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
import aio_pika, uuid

from app.config import settings
from app.db import engine, session_maker, SessionDep
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


class OrderCreate(BaseModel):
    amount: int

class OrderRead(BaseModel):
    id: int
    status: str
    amount: int

    model_config = {"from_attributes": True}

class PaymentResult(BaseModel):
    order_id: int
    status: str
    message_id: str


@app.post("/orders/{order_id}/pay", response_model=PaymentResult)
async def pay_order(order_id: int, request: Request, session: SessionDep):
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
    return PaymentResult(order_id=order_id, status="paid", message_id=message_id)



@app.post("/orders", status_code=201, response_model=OrderRead)
async def create_order(data: OrderCreate, session: SessionDep):
    async with session.begin():
        order = Order(status="pending", amount=data.amount)
        session.add(order)
    return order