from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from fastapi import Depends
from typing import Annotated

from shared.config import settings



engine = create_async_engine(settings.database_url)

session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def get_session():
    async with session_maker() as session:
        yield session

SessionDep = Annotated[AsyncSession, Depends(get_session)]