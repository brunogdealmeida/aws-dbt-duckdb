# Local API only ("quack on demand") — build context is this directory
# (docker-compose.yml builds with `context: .` from here), NOT the repo
# root, since this image only needs query_service/'s own files.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common.py db.py local_api.py ./
COPY static ./static

EXPOSE 8000
CMD ["uvicorn", "local_api:app", "--host", "0.0.0.0", "--port", "8000"]
