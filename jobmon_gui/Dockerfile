# syntax=docker/dockerfile:1
# Start from a specific Bun version for stability
FROM oven/bun:1.1.13 AS dependencies

# Set the working directory
WORKDIR /app

# Copy only package files first (for better caching)
COPY package.json bun.lockb* ./

# Install dependencies with cache mount for faster rebuilds
RUN --mount=type=cache,target=/root/.bun/install/cache \
    --mount=type=cache,target=/app/node_modules/.cache \
    bun install

# Build stage
FROM oven/bun:1.1.13 AS builder

WORKDIR /app

# Copy dependencies from previous stage
COPY --from=dependencies /app/node_modules ./node_modules
COPY --from=dependencies /app/package.json ./package.json

# Copy source code
COPY . .

# Build the production bundle
RUN bun run build

# Production stage
FROM oven/bun:1.1.13 AS runtime

WORKDIR /app

# Copy built application
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./package.json

# Install only serve (production server)
RUN --mount=type=cache,target=/root/.bun/install/cache \
    bun add serve

# Expose the production port
EXPOSE 3000

# Serve the built files
CMD ["bun", "run", "serve"]
