FROM rust:latest

# Install system dependencies
RUN apt-get update && apt-get install -y \
    make \
    python3 \
    coreutils \
    pkg-config \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /workspace

# Copy project files
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY tests ./tests
COPY scripts ./scripts

# Copy Makefile for orchestration
COPY testrunner/Makefile /testrunner/Makefile

# Create output directory
RUN mkdir -p /output

# Install cntryl-stress early for stress tests
RUN cargo install cntryl-stress 2>&1 | head -20

# Set entrypoint to run Makefile
ENTRYPOINT ["bash", "-c"]
CMD ["make -f /testrunner/Makefile all"]