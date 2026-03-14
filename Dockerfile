# Build stage: install build deps and compile wheels
FROM python:3.14-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev libpq-dev

WORKDIR /app
COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# Runtime stage: only copy prebuilt wheels
FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/* && rm -rf /wheels

COPY . .

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--control-socket", "/tmp/gunicorn.ctl", "run:app"]
