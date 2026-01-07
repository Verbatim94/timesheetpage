FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

WORKDIR /app

# Copy requirements first for cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install gunicorn

# Copy application code
COPY . .

# Create a non-root user (optional but good practice, though Playwright often needs specific permissions, 
# the base image is set up for root usually, but let's stick to standard practice if possible. 
# For simplicity with Playwright in Docker, running as root in the official image is often the path of least resistance for generated PDFs).
# We will stick to the default user of the base image (root) to avoid permission issues with the browser installation.

# Expose port (Render ignores this, but useful for local documentation)
EXPOSE 8107

# Run application with Gunicorn
# Uses PORT env var if set (Render), otherwise defaults to 8107 (Local)
# Must use uvicorn worker for FastAPI (ASGI)
CMD ["sh", "-c", "gunicorn app:app --workers 1 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:${PORT:-8107} --timeout 180"]
