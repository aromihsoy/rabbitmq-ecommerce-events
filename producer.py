import asyncio

import aio_pika




async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/"
    )

    async with connection:
        channel = await connection.channel()

        exchange = await channel.declare_exchange("orders", aio_pika.ExchangeType.TOPIC)

        await exchange.publish(
            aio_pika.Message(body=b"order paid"),
            routing_key="order.paid",
        )

if __name__ == "__main__":
    asyncio.run(main())