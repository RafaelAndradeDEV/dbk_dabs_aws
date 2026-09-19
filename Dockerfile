# Start from the official uv image
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install system dependencies required by multiple steps
# Running them all in one layer is more efficient
RUN apt-get update && apt-get install -y \
    git \
    curl \
    unzip \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install the Databricks CLI
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
