from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.twitterapi_io_social_ingestion import TWITTERAPI_IO_WS_URL  # noqa: E402


@dataclass
class HandshakeResult:
    mode: str
    handshake_ok: bool = False
    failed: bool = False
    status_code: int | None = None
    close_status_code: int | None = None
    close_msg: str | None = None
    event_types: list[str] = field(default_factory=list)
    cf_ray: str | None = None
    date: str | None = None
    error_type: str | None = None
    error: str | None = None


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Isolated TwitterAPI.io WebSocket handshake probe")
    parser.add_argument("--mode", choices=["official", "production", "both"], default="both")
    parser.add_argument("--ws-url", default=TWITTERAPI_IO_WS_URL)
    parser.add_argument("--wait-seconds", type=float, default=30)
    parser.add_argument("--between-probe-wait-seconds", type=float, default=90)
    args = parser.parse_args()

    api_key = os.getenv("TWITTERAPI_IO_API_KEY", "")
    if not api_key:
        print("TWITTERAPI_IO_API_KEY missing; WebSocket probe skipped.")
        return

    modes = ["official", "production"] if args.mode == "both" else [args.mode]
    for index, mode in enumerate(modes):
        if index > 0:
            print(f"Waiting {args.between_probe_wait_seconds:.0f}s before next probe to avoid connection-slot overlap.")
            time.sleep(max(0, args.between_probe_wait_seconds))
        if mode == "official":
            result = run_official_style(args.ws_url, api_key, args.wait_seconds)
        else:
            result = run_production_style(args.ws_url, api_key, args.wait_seconds)
        print_result(result)


def run_official_style(ws_url: str, api_key: str, wait_seconds: float) -> HandshakeResult:
    try:
        import websocket
    except ImportError:
        return HandshakeResult("official", failed=True, error_type="ImportError", error="websocket-client missing")

    result = HandshakeResult("official")
    stop = threading.Event()

    def on_open(ws):  # noqa: ANN001
        result.handshake_ok = True
        print("OFFICIAL HANDSHAKE_OK")

    def on_message(ws, message):  # noqa: ANN001
        event_type = _event_type(message)
        if event_type:
            result.event_types.append(event_type)
            print(f"OFFICIAL event_type={event_type}")
        if event_type in {"connected", "ping"}:
            ws.close()
            stop.set()

    def on_error(ws, error):  # noqa: ANN001
        _record_error(result, error)
        stop.set()

    def on_close(ws, close_status_code, close_msg):  # noqa: ANN001
        result.close_status_code = close_status_code
        result.close_msg = _safe_text(close_msg)
        stop.set()

    ws = websocket.WebSocketApp(
        ws_url,
        header={"x-api-key": api_key},
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    thread = threading.Thread(target=ws.run_forever, daemon=True)
    thread.start()
    stop.wait(timeout=max(1, wait_seconds))
    try:
        ws.close()
    except Exception:
        pass
    thread.join(timeout=5)
    if not result.handshake_ok and not result.failed:
        result.failed = True
        result.error_type = "Timeout"
        result.error = "handshake/event timeout"
    return result


def run_production_style(ws_url: str, api_key: str, wait_seconds: float) -> HandshakeResult:
    try:
        import websocket
    except ImportError:
        return HandshakeResult("production", failed=True, error_type="ImportError", error="websocket-client missing")

    result = HandshakeResult("production")
    ws = None
    try:
        ws = websocket.create_connection(
            ws_url,
            header=[f"x-api-key: {api_key}"],
            timeout=10,
        )
        result.handshake_ok = True
        print("PRODUCTION HANDSHAKE_OK")
        deadline = time.monotonic() + max(1, wait_seconds)
        while time.monotonic() < deadline:
            try:
                message = ws.recv()
            except (TimeoutError, websocket.WebSocketTimeoutException):
                continue
            event_type = _event_type(message)
            if event_type:
                result.event_types.append(event_type)
                print(f"PRODUCTION event_type={event_type}")
            if event_type in {"connected", "ping"}:
                break
    except Exception as exc:
        _record_error(result, exc)
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
    return result


def print_result(result: HandshakeResult) -> None:
    status = "HANDSHAKE_OK" if result.handshake_ok else "HANDSHAKE_FAILED"
    print(f"\n=== {result.mode.upper()} WebSocket Probe ===")
    print(f"result={status}")
    if result.status_code is not None:
        print(f"status={result.status_code}")
    if result.cf_ray:
        print(f"cf-ray={result.cf_ray}")
    if result.date:
        print(f"date={result.date}")
    print(f"event_types={result.event_types or 'NONE'}")
    if result.close_status_code is not None:
        print(f"close_code={result.close_status_code}")
    if result.close_msg:
        print(f"close_msg={result.close_msg}")
    if result.error_type:
        print(f"error_type={result.error_type}")
    if result.error:
        print(f"error={result.error}")


def _record_error(result: HandshakeResult, error: Exception) -> None:
    result.failed = True
    result.error_type = error.__class__.__name__
    status_code = getattr(error, "status_code", None)
    if status_code is not None:
        result.status_code = int(status_code)
    headers = getattr(error, "resp_headers", None) or getattr(error, "headers", None)
    if isinstance(headers, dict):
        result.cf_ray = _header(headers, "cf-ray")
        result.date = _header(headers, "date")
    if result.status_code is not None:
        result.error = "websocket handshake rejected"
    else:
        result.error = _safe_text(str(error))


def _event_type(message: str | bytes) -> str | None:
    try:
        text = message.decode("utf-8") if isinstance(message, bytes) else message
        data = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    value = data.get("event_type") or data.get("eventType") or data.get("type")
    return str(value) if value else None


def _header(headers: dict[str, Any], name: str) -> str | None:
    for key, value in headers.items():
        if str(key).lower() == name:
            return str(value)
    return None


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    blocked = ("cookie", "set-cookie", "x-api-key", "authorization")
    lines = [line for line in text.splitlines() if not any(item in line.lower() for item in blocked)]
    return " ".join(lines)[:500]


if __name__ == "__main__":
    main()
