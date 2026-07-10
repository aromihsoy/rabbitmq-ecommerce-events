from datetime import datetime
from sqlalchemy import String, DateTime, func, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column




class Base(DeclarativeBase):
    pass



class ProcessedMessage(Base):
    __tablename__ = "processed_messages"

    consumer: Mapped[str] = mapped_column(String, primary_key=True)
    message_id: Mapped[str] = mapped_column(String, primary_key=True)
    processed_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now())


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column(default="pending")
    amount: Mapped[int]


class Outbox(Base):
    __tablename__ = "outbox"

    message_id: Mapped[str] = mapped_column(primary_key=True)
    routing_key: Mapped[str] = mapped_column(String)
    payload: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index(
            "ix_outbox_unpublished",
            "created_at",
            postgresql_where=(published_at.is_(None))
        ),
    )