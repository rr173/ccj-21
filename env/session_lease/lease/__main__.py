"""启动入口：python3 -m lease --db lease.db --port 8080"""
from __future__ import annotations

import argparse

from . import config
from .api import build_server
from .store import Store


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=config.DB_PATH)
    parser.add_argument("--host", default=config.HTTP_HOST)
    parser.add_argument("--port", type=int, default=config.HTTP_PORT)
    args = parser.parse_args()

    store = Store(args.db)
    server = build_server(store, host=args.host, port=args.port)
    print(f"session-lease service on http://{args.host}:{args.port}"
          f" (db={args.db})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
