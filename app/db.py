from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.config import settings



engine = create_async_engine(settings.database_url)

session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


