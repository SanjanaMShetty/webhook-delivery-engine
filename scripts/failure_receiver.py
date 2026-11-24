"""Local-only HTTP receiver for simulating transient webhook failures."""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock


class FailureReceiver(BaseHTTPRequestHandler):
    attempts = 0
    lock = Lock()
    fail_first = 2
    failure_status = 500

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        with self.lock:
            type(self).attempts += 1
            attempt = type(self).attempts
        failing = attempt <= self.fail_first
        status = self.failure_status if failing else 200
        response = (
            b"simulated transient failure" if failing else b"received after retry"
        )
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)
        print(f"attempt={attempt} status={status} bytes={len(body)}", flush=True)

    def log_message(self, format: str, *args: object) -> None:
        print(format % args, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--fail-first", type=int, default=2)
    parser.add_argument("--status", type=int, default=500)
    args = parser.parse_args()
    if args.fail_first < 0:
        parser.error("--fail-first must be zero or greater")
    if args.status < 400 or args.status > 599:
        parser.error("--status must be an HTTP error status (400-599)")
    FailureReceiver.fail_first = args.fail_first
    FailureReceiver.failure_status = args.status
    server = ThreadingHTTPServer((args.host, args.port), FailureReceiver)
    print(
        f"Failure receiver listening at http://{args.host}:{args.port}; first {args.fail_first} request(s) return {args.status}, then 200",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping failure receiver", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
