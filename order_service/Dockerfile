FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY shared/ ./shared/
COPY order_service/ ./order_service/
COPY alembic/ ./alembic/
COPY alembic.ini .


CMD ["uvicorn", "order_service.main:app", "--host", "0.0.0.0", "--port", "8000"]