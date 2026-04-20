# ── Stage 1: dependency resolution ───────────────────────────────────────────
# Use the official uv image to compile the lockfile into a venv.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /app

# Copy only dependency manifests first (layer caching — changes to src code
# won't invalidate this layer unless dependencies change)
COPY pyproject.toml uv.lock* ./

# Install dependencies into /app/.venv without the project itself
RUN uv sync --frozen --no-install-project

# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm AS runtime

# Install minimal system deps for psycopg2 (libpq)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy venv from builder stage (lean final image)
COPY --from=builder /app/.venv /app/.venv

# Copy source code
COPY src/ ./src/
COPY pyproject.toml ./

# Make the venv's Python the default interpreter
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# Default: run the full pipeline for all configured years.
# Override with --years <year> for incremental loads.
CMD ["python", "-m", "src.pipeline.main"]
