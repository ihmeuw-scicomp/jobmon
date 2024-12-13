# Start from a specific Bun version for stability
FROM oven/bun:1.1.13

# Create app directory
WORKDIR /app

# Copy all files
COPY . /app

# Install dependencies
RUN bun install

# Build the production bundle
RUN bun run build

# Install a static server (e.g. serve)
RUN bun add serve

# Expose the production port
EXPOSE 3000

# Serve the built files
CMD ["bun", "run", "serve"]
