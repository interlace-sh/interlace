# ---- Build stage ----
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
COPY . ./
RUN pip install --no-cache-dir ".[service]"

# ---- Runtime stage ----
FROM python:3.12-slim

LABEL maintainer="Mark <mark@interlace.sh>"
LABEL description="Interlace - A modern data pipeline framework"
LABEL org.opencontainers.image.source="https://github.com/interlace-sh/interlace"

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /project

# `interlace serve` binds 8000. A container must pass --host 0.0.0.0 or nothing outside can connect.
EXPOSE 8000

ENTRYPOINT ["interlace"]
CMD ["--help"]
