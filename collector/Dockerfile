FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml .
COPY collector/ collector/

RUN pip install --no-cache-dir .

CMD ["python", "-m", "collector"]
