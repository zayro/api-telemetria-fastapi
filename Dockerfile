FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py app_metrics.py telemetry.py ./
COPY api/ api/
COPY application/ application/
COPY domain/ domain/
COPY infrastructure/ infrastructure/
CMD ["python", "app.py"]
