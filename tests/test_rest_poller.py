from __future__ import annotations

import asyncio
import importlib.util
import json
import stat
import sys
import tempfile
import types
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _load_adapter_module():
    gateway = types.ModuleType("gateway")
    gateway_config = types.ModuleType("gateway.config")
    gateway_platforms = types.ModuleType("gateway.platforms")
    gateway_base = types.ModuleType("gateway.platforms.base")

    class Platform:
        def __new__(cls, value):
            return value

    @dataclass
    class PlatformConfig:
        extra: dict
        typing_indicator: bool = False

    class ProcessingOutcome(Enum):
        SUCCESS = "success"
        FAILURE = "failure"
        CANCELLED = "cancelled"

    class MessageType:
        TEXT = "text"

    class MessageEvent:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    @dataclass
    class SendResult:
        success: bool
        message_id: str | None = None
        error: str | None = None

    class BasePlatformAdapter:
        def __init__(self, config, platform):
            self.config = config
            self.platform = platform
            self.name = str(platform)
            self._running = False
            self._active_sessions = {}
            self.connected_count = 0
            self.fatal_error = None
            self.fatal_notified = False

        async def on_processing_complete(self, event, outcome):
            return None

        def _set_fatal_error(self, code, message, retryable=False):
            self._running = False
            self.fatal_error = (code, message, retryable)

        async def _notify_fatal_error(self):
            self.fatal_notified = True

        def _mark_connected(self):
            self._running = True
            self.connected_count += 1

        def _mark_disconnected(self):
            self._running = False

        def _write_runtime_status_safe(self, context, **kwargs):
            return None

    setattr(gateway_config, "Platform", Platform)
    setattr(gateway_config, "PlatformConfig", PlatformConfig)
    setattr(gateway_base, "BasePlatformAdapter", BasePlatformAdapter)
    setattr(gateway_base, "MessageEvent", MessageEvent)
    setattr(gateway_base, "MessageType", MessageType)
    setattr(gateway_base, "ProcessingOutcome", ProcessingOutcome)
    setattr(gateway_base, "SendResult", SendResult)
    setattr(
        gateway_base,
        "build_session_key",
        lambda source, **kwargs: str(getattr(source, "chat_id", "mailbox")),
    )

    sys.modules["gateway"] = gateway
    sys.modules["gateway.config"] = gateway_config
    sys.modules["gateway.platforms"] = gateway_platforms
    sys.modules["gateway.platforms.base"] = gateway_base

    spec = importlib.util.spec_from_file_location(
        "openmail_rest_poller_adapter_under_test", ROOT / "adapter.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, PlatformConfig, ProcessingOutcome, MessageEvent


adapter_module, PlatformConfig, ProcessingOutcome, MessageEvent = _load_adapter_module()


def _config(root: Path, **extra):
    payload = {
        "api_key": "test-key",
        "inbox_ids": ["inbox-1"],
        "api_base_url": "https://api.openmail.invalid",
        "poll_interval_seconds": 120,
        "poll_jitter_seconds": 0,
        "poll_state_path": str(root / "poller_state.json"),
        "legacy_pending_journal_path": str(root / "pending_notifications.json"),
        "notification_batch_window_seconds": 0,
        "notification_min_interval_seconds": 0,
    }
    payload.update(extra)
    return PlatformConfig(extra=payload)


def _adapter(root: Path, **extra):
    adapter = adapter_module.OpenMailMailboxAdapter(_config(root, **extra))
    adapter._ensure_flush_task_locked = lambda: None
    return adapter


def _summary(count: int, at: str) -> dict:
    return {
        "id": "thread-1",
        "messageCount": count,
        "lastMessageAt": at,
        "createdAt": "2026-08-19T00:00:00Z",
        "subject": "Test",
        "isRead": False,
    }


def _message(
    message_id: str,
    *,
    thread_id: str = "thread-1",
    direction: str = "inbound",
    created_at: str = "2026-08-19T00:00:00Z",
) -> dict:
    return {
        "id": message_id,
        "threadId": thread_id,
        "inboxId": "inbox-1",
        "direction": direction,
        "fromAddr": "sender@example.invalid",
        "subject": "Test",
        "createdAt": created_at,
        "bodyText": "untrusted body must not enter poll state",
        "bodyHtml": "<p>untrusted body must not enter poll state</p>",
        "attachments": [],
    }


async def _baseline(adapter) -> None:
    async def summaries(_inbox_id: str):
        return {"thread-1": _summary(1, "2026-08-19T00:00:00Z")}

    async def messages(_inbox_id: str):
        return [_message("message-old")]

    adapter._fetch_all_thread_summaries = summaries
    adapter._fetch_all_inbound_messages = messages
    assert await adapter._poll_once() == 0


def test_source_and_manifest_are_rest_only() -> None:
    source = (ROOT / "adapter.py").read_text(encoding="utf-8")
    manifest = (ROOT / "plugin.yaml").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "import websockets" not in source
    assert "DEFAULT_WS_URL" not in source
    assert "_connect_and_consume" not in source
    assert "_handle_ws_message" not in source
    assert "last_event_id" not in source
    assert "version: 0.2.0" in manifest
    assert "REST" in manifest
    assert "WebSocket" not in manifest
    assert "REST-only" in readme


def test_first_poll_takes_a_stable_ids_only_baseline_without_dispatching() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = _adapter(root)
            summary_calls = 0

            async def summaries(_inbox_id: str):
                nonlocal summary_calls
                summary_calls += 1
                return {"thread-1": _summary(1, "2026-08-19T00:00:00Z")}

            async def messages(_inbox_id: str):
                return [_message("message-old")]

            adapter._fetch_all_thread_summaries = summaries
            adapter._fetch_all_inbound_messages = messages

            assert await adapter._poll_once() == 0
            assert summary_calls == 2, "baseline must bracket the message scan with stable thread snapshots"
            assert adapter._pending_notifications == []

            state_path = root / "poller_state.json"
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            thread = payload["inboxes"]["inbox-1"]["threads"]["thread-1"]
            assert thread["inbound_message_ids"] == ["message-old"]
            assert thread["message_count"] == 1
            assert payload["pending"] == {}
            serialized = state_path.read_text(encoding="utf-8")
            assert "untrusted body" not in serialized
            assert "bodyText" not in serialized
            assert stat.S_IMODE(state_path.stat().st_mode) == 0o600

    asyncio.run(scenario())


def test_changed_thread_is_durably_owned_before_dispatch_and_is_not_duplicated() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = _adapter(root)
            await _baseline(adapter)

            async def changed_summaries(_inbox_id: str):
                return {"thread-1": _summary(2, "2026-08-19T00:01:00Z")}

            fetch_calls = 0

            async def changed_messages(thread_id: str):
                nonlocal fetch_calls
                assert thread_id == "thread-1"
                fetch_calls += 1
                return [
                    _message("message-old"),
                    _message("message-new", created_at="2026-08-19T00:01:00Z"),
                ]

            adapter._fetch_all_thread_summaries = changed_summaries
            adapter._fetch_thread_messages = changed_messages

            assert await adapter._poll_once() == 1
            assert fetch_calls == 1
            payload = json.loads((root / "poller_state.json").read_text(encoding="utf-8"))
            assert list(payload["pending"]) == ["message-new"]
            assert payload["inboxes"]["inbox-1"]["threads"]["thread-1"]["inbound_message_ids"] == [
                "message-new",
                "message-old",
            ]
            assert [item["message_id"] for item in adapter._pending_notifications] == ["message-new"]
            assert "untrusted body" not in json.dumps(payload)

            async def should_not_fetch(_thread_id: str):
                raise AssertionError("unchanged thread must not be fetched")

            adapter._fetch_thread_messages = should_not_fetch
            assert await adapter._poll_once() == 0
            assert [item["message_id"] for item in adapter._pending_notifications] == ["message-new"]

    asyncio.run(scenario())


def test_state_write_failure_does_not_advance_checkpoint_or_queue_notification(monkeypatch) -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = _adapter(root)
            await _baseline(adapter)
            before_disk = (root / "poller_state.json").read_text(encoding="utf-8")
            before_memory = json.dumps(adapter._state, sort_keys=True, default=str)

            async def changed_summaries(_inbox_id: str):
                return {"thread-1": _summary(2, "2026-08-19T00:01:00Z")}

            async def changed_messages(_thread_id: str):
                return [_message("message-old"), _message("message-new")]

            adapter._fetch_all_thread_summaries = changed_summaries
            adapter._fetch_thread_messages = changed_messages
            monkeypatch.setattr(
                adapter_module,
                "_write_poll_state",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("simulated state failure")),
            )

            with pytest.raises(OSError, match="simulated state failure"):
                await adapter._poll_once()

            assert (root / "poller_state.json").read_text(encoding="utf-8") == before_disk
            assert json.dumps(adapter._state, sort_keys=True, default=str) == before_memory
            assert adapter._pending_notifications == []

    asyncio.run(scenario())


def test_pending_notification_survives_restart_and_success_acknowledges_it() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = _adapter(root)
            await _baseline(first)

            async def changed_summaries(_inbox_id: str):
                return {"thread-1": _summary(2, "2026-08-19T00:01:00Z")}

            async def changed_messages(_thread_id: str):
                return [_message("message-old"), _message("message-new")]

            first._fetch_all_thread_summaries = changed_summaries
            first._fetch_thread_messages = changed_messages
            assert await first._poll_once() == 1

            restarted = _adapter(root)
            await restarted._load_state_for_poll()
            assert [item["message_id"] for item in restarted._pending_notifications] == ["message-new"]

            batch = MessageEvent(
                raw_message={
                    "event_type": "openmail.mailbox.batch",
                    "message_ids": ["message-new"],
                },
                source=SimpleNamespace(chat_id="mailbox"),
            )
            await restarted.on_processing_complete(batch, ProcessingOutcome.SUCCESS)

            payload = adapter_module._read_poll_state(str(root / "poller_state.json"))
            assert payload["pending"] == {}
            assert restarted._pending_notifications == []

    asyncio.run(scenario())


def test_failed_turn_keeps_exactly_one_durable_retry() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = _adapter(root)
            await _baseline(adapter)

            async def changed_summaries(_inbox_id: str):
                return {"thread-1": _summary(2, "2026-08-19T00:01:00Z")}

            async def changed_messages(_thread_id: str):
                return [_message("message-old"), _message("message-new")]

            adapter._fetch_all_thread_summaries = changed_summaries
            adapter._fetch_thread_messages = changed_messages
            assert await adapter._poll_once() == 1
            adapter._pending_notifications.clear()

            batch = MessageEvent(
                raw_message={
                    "event_type": "openmail.mailbox.batch",
                    "message_ids": ["message-new"],
                },
                source=SimpleNamespace(chat_id="mailbox"),
            )
            await adapter.on_processing_complete(batch, ProcessingOutcome.FAILURE)
            await adapter.on_processing_complete(batch, ProcessingOutcome.FAILURE)

            assert [item["message_id"] for item in adapter._pending_notifications] == ["message-new"]
            payload = adapter_module._read_poll_state(str(root / "poller_state.json"))
            assert list(payload["pending"]) == ["message-new"]

    asyncio.run(scenario())


def test_corrupt_poll_state_fails_closed() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "poller_state.json"
            state_path.write_text("not json\n", encoding="utf-8")
            adapter = _adapter(root)
            with pytest.raises(json.JSONDecodeError):
                await adapter._load_state_for_poll()

    asyncio.run(scenario())


def test_connect_does_not_claim_readiness_until_initial_rest_poll_succeeds() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adapter = _adapter(Path(tmp))

            async def fail_poll():
                raise RuntimeError("provider unavailable")

            adapter._poll_once = fail_poll
            assert await adapter.connect() is False
            assert adapter.connected_count == 0
            assert adapter.fatal_error is not None
            assert adapter.fatal_error[0] == "openmail_rest_initial_poll_failed"
            assert adapter.fatal_error[2] is True

    asyncio.run(scenario())


def test_runtime_poll_failure_notifies_gateway_reconnect_supervisor() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adapter = _adapter(
                Path(tmp), poll_interval_seconds=0, poll_jitter_seconds=0
            )
            adapter._running = True

            async def fail_poll():
                raise RuntimeError("provider unavailable")

            adapter._poll_once = fail_poll
            await adapter._poll_loop()
            assert adapter.fatal_error is not None
            assert adapter.fatal_error[0] == "openmail_rest_poll_failed"
            assert adapter.fatal_error[2] is True
            assert adapter.fatal_notified is True
            assert adapter._running is False

    asyncio.run(scenario())


def test_rest_endpoints_are_paginated_and_thread_fetch_is_scoped() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adapter = _adapter(Path(tmp))
            calls: list[tuple[str, dict]] = []

            async def request(path: str, *, params=None, max_bytes=None):
                params = dict(params or {})
                calls.append((path, params))
                if path.endswith("/threads"):
                    offset = int(params["offset"])
                    return {
                        "data": [
                            {
                                "id": f"thread-{offset + 1}",
                                "messageCount": 1,
                                "lastMessageAt": f"2026-08-19T00:0{offset}:00Z",
                                "createdAt": "2026-08-19T00:00:00Z",
                                "subject": "Test",
                                "isRead": False,
                            }
                        ],
                        "total": 2,
                    }
                if path.endswith("/messages") and "/inboxes/" in path:
                    return {"data": [_message("message-1")], "total": 1}
                if path == "/v1/threads/thread-1/messages":
                    return {"data": [_message("message-1")]}
                raise AssertionError(path)

            adapter._request_json = request
            summaries = await adapter._fetch_all_thread_summaries("inbox/1")
            baseline = await adapter._fetch_all_inbound_messages("inbox/1")
            thread = await adapter._fetch_thread_messages("thread-1")

            assert set(summaries) == {"thread-1", "thread-2"}
            assert [row["id"] for row in baseline] == ["message-1"]
            assert [row["id"] for row in thread] == ["message-1"]
            assert calls[0][0] == "/v1/inboxes/inbox%2F1/threads"
            assert [int(params["offset"]) for path, params in calls if path.endswith("/threads")] == [0, 1]
            inbox_message_call = next(
                (path, params)
                for path, params in calls
                if path.endswith("/messages") and "/inboxes/" in path
            )
            assert inbox_message_call[0] == "/v1/inboxes/inbox%2F1/messages"
            assert inbox_message_call[1]["direction"] == "inbound"
            assert calls[-1][0] == "/v1/threads/thread-1/messages"

    asyncio.run(scenario())
