FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY migrations /app/migrations
COPY prompts /app/prompts
COPY scripts /app/scripts

RUN mkdir -p /app/config /data /logs /backups && chmod +x /app/scripts/hostinger_first_boot.sh

CMD ["sleep", "infinity"]
