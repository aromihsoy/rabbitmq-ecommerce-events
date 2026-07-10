import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from shared.models import Base



TEST_DB_URL = "postgresql+asyncpg://ecommerce:ecommerce@localhost:5432/ecommerce_test"


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine(TEST_DB_URL)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    
    await engine.dispose()