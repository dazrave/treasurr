FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY treasurr/ ./treasurr/
RUN pip install --no-cache-dir .

RUN mkdir -p /app/data

EXPOSE 8080

CMD ["python", "-m", "treasurr", "serve"]
