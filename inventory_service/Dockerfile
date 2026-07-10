FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY shared/ ./shared/
COPY inventory_service/ ./inventory_service/


CMD ["python", "-m", "inventory_service.consumer"]