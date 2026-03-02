FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY treasurr/ ./treasurr/

RUN mkdir -p /app/data

EXPOSE 8080

CMD ["python", "-m", "treasurr", "serve"]
