"""CLI entry point: python -m treasurr serve"""

from __future__ import annotations

import logging
import sys

import uvicorn

from treasurr.config import load_config


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if len(sys.argv) < 2 or sys.argv[1] != "serve":
        print("Usage: python -m treasurr serve [--config config.yaml]")
        sys.exit(1)

    config_path = "config.yaml"
    if "--config" in sys.argv:
        idx = sys.argv.index("--config")
        if idx + 1 < len(sys.argv):
            config_path = sys.argv[idx + 1]

    config = load_config(config_path)

    from treasurr.app import create_app
    app = create_app(config)

    uvicorn.run(app, host=config.host, port=config.port, log_level="info")


if __name__ == "__main__":
    main()
