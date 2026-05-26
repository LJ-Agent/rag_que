FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY src/ ./src/
RUN pip install --no-cache-dir --user .

FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /root/.local /root/.local

COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/
COPY proto/ ./proto/

ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

EXPOSE 50055

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=30s \
    CMD python -c "import grpc; ch=grpc.insecure_channel('localhost:50055'); grpc.channel_ready_future(ch).result(timeout=3)"

ENTRYPOINT ["python", "src/main.py"]
