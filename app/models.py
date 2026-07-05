from datetime import datetime
from sqlalchemy import String, DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column




class Base(DeclarativeBase):
    pass



class ProcessedMessage(Base):
    __tablename__ = "processed_messages"

    consumer: Mapped[str] = mapped_column(String, primary_key=True)
    message_id: Mapped[str] = mapped_column(String, primary_key=True)
    processed_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now())