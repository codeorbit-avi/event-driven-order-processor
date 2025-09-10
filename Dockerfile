# ---- build/runtime image ----
FROM python:3.12-slim

# System deps (optional but good to have tzdata)
RUN apt-get update && apt-get install -y --no-install-recommends tzdata curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy code
COPY app.py .

# envs (override at runtime if you want)
ENV PORT=8000
EXPOSE 8000

# run
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
