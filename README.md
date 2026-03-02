# Treasurr

**Your treasure. Your crew. Your plunder.**

Plex storage quota manager with a pirate theme. Each user gets a virtual storage quota — content they request counts against their "treasure chest." When crew members watch it too, it gets promoted to the "shared plunder," freeing the requester's quota.

## Quick Start

```bash
cp config.example.yaml config.yaml
cp .env.example .env
# Edit config.yaml and .env with your API details
docker compose up -d
```

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
python -m treasurr serve --config config.example.yaml
```

## License

MIT
