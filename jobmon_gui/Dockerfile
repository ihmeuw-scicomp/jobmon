# Use the official Bun image
FROM oven/bun:latest

# Set the working directory
WORKDIR /app

# Copy application files
COPY . .

# Install dependencies
RUN bun install

# Expose the port your app will run on
EXPOSE 3000

# Start the Bun app
CMD ["bun", "run", "start"]
