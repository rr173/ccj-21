"""启动遥测告警服务。

用法:
    python -m telemetry --db telemetry.db --host 127.0.0.1 --port 8080 \
        [--webhook URL] [--ack-timeout 30]
"""
from __future__ import annotations

import argparse

from .api import serve
from .notify import HttpWebhookSender


def main(argv=None):
    parser = argparse.ArgumentParser(prog="telemetry", description=__doc__)
    parser.add_argument("--db", default="telemetry.db", help="SQLite 数据库路径")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--webhook", default=None,
                        help="告警通知投递的 webhook URL（缺省时通知仅落库）")
    parser.add_argument("--ack-timeout", type=float, default=30.0,
                        help="通知确认超时秒数，超时后重投")
    args = parser.parse_args(argv)

    sender = HttpWebhookSender(args.webhook) if args.webhook else None
    serve(db_path=args.db, host=args.host, port=args.port, sender=sender)


if __name__ == "__main__":
    main()
