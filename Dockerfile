# SnowDucks Docker Image
# Always builds for linux/amd64 (x86_64). If you are on Apple Silicon, Docker Desktop will use emulation.

FROM python:3.11-slim

# Install system dependencies (minimal set)
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    pkg-config \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Install Python dependencies (this includes ADBC libraries)
RUN pip install --upgrade pip \
    && pip install -e ./cli \
    && pip install adbc-driver-manager adbc-driver-snowflake

# Build C++ extension (will use Python-installed ADBC libraries)
WORKDIR /app/ui/extension-template
RUN make
WORKDIR /app

# Entrypoint is handled by Makefile targets
CMD ["make", "help"] 