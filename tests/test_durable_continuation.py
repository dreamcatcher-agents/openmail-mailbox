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

        async def on_processing_complete(self, event, outcome):
            return None

        def _set_fatal_error(self, code, message, retryable=False):
            self.fatal_error = (code, message, retryable)

        def _mark_connected(self):
            return None

        def _mark_disconnected(self):
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

    spec = importlib.util.spec_from_file_location("openmail_mailbox_adapter_under_test", ROOT / "adapter.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, PlatformConfig, ProcessingOutcome, MessageEvent


adapter_module, PlatformConfig, ProcessingOutcome, MessageEvent = _load_adapter_module()


def _config(root: Path):
    return PlatformConfig(
        extra={
            "api_key": "test-key",
            "inbox_ids": ["inbox-1"],
            "last_event_id_path": str(root / "last_event_id.txt"),
            "pending_journal_path": str(root / "pending_notifications.json"),
            "notification_batch_window_seconds": 0,
            "notification_min_interval_seconds": 0,
        }
    )


def _event(event_id: str = "evt-1") -> dict:
    return {
        "event": "message.received",
        "event_id": event_id,
        "inbox_id": "inbox-1",
        "message": {
            "id": f"msg-{event_id}",
            "thread_id": "thread-1",
            "inbox_id": "inbox-1",
            "from": "sender@example.invalid",
            "subject": "Follow up",
            "body_text": "untrusted body must not enter the journal",
            "received_at": "2026-08-10T00:00:00Z",
        },
    }


def _adapter(root: Path):
    adapter = adapter_module.OpenMailMailboxAdapter(_config(root))
    adapter._ensure_flush_task_locked = lambda: None
    return adapter


def test_notification_is_durable_before_cursor_and_body_is_redacted() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        adapter = _adapter(root)
        order: list[str] = []
        original_write_cursor = adapter_module._write_last_event_id

        def record_cursor(path: str, event_id: str) -> None:
            assert (root / "pending_notifications.json").is_file()
            order.append(f"cursor:{event_id}")
            original_write_cursor(path, event_id)

        setattr(adapter_module, "_write_last_event_id", record_cursor)
        try:
            asyncio.run(adapter._handle_ws_message(json.dumps(_event())))
        finally:
            setattr(adapter_module, "_write_last_event_id", original_write_cursor)

        assert order == ["cursor:evt-1"]
        journal_path = root / "pending_notifications.json"
        payload = json.loads(journal_path.read_text(encoding="utf-8"))
        assert [row["event_id"] for row in payload["notifications"]] == ["evt-1"]
        serialized = journal_path.read_text(encoding="utf-8")
        assert "untrusted body must not enter the journal" not in serialized
        assert "body_text_present" in serialized
        assert stat.S_IMODE(journal_path.stat().st_mode) == 0o600
        assert (root / "last_event_id.txt").read_text(encoding="utf-8").strip() == "evt-1"


def test_journal_failure_does_not_advance_cursor_or_poison_dedupe() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        adapter = _adapter(root)
        cursor_calls: list[str] = []
        original_queue = adapter._queue_mail_notification
        original_write_cursor = adapter_module._write_last_event_id

        async def fail_queue(data, *, event_id):
            raise OSError("simulated journal failure")

        adapter._queue_mail_notification = fail_queue
        setattr(adapter_module, "_write_last_event_id", lambda path, event_id: cursor_calls.append(event_id))
        try:
            try:
                asyncio.run(adapter._handle_ws_message(json.dumps(_event())))
            except OSError as exc:
                assert "simulated journal failure" in str(exc)
            else:
                raise AssertionError("journal failure should propagate to reconnect logic")
        finally:
            adapter._queue_mail_notification = original_queue
            setattr(adapter_module, "_write_last_event_id", original_write_cursor)

        assert cursor_calls == []
        assert adapter._was_seen("evt-1") is False


def test_crash_recovery_replays_once_then_success_acknowledges() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        first = _adapter(root)
        asyncio.run(first._queue_mail_notification(_event(), event_id="evt-1"))

        recovered = adapter_module._read_pending_journal(str(root / "pending_notifications.json"))
        assert [item["event_id"] for item in recovered] == ["evt-1"]

        restarted = _adapter(root)
        restarted._journal_notifications = {item["event_id"]: item for item in recovered}
        restarted._pending_notifications = list(recovered)
        batch = MessageEvent(
            raw_message={"event_type": "openmail.mailbox.batch", "event_ids": ["evt-1"]},
            source=SimpleNamespace(chat_id="mailbox"),
        )
        asyncio.run(restarted.on_processing_complete(batch, ProcessingOutcome.SUCCESS))

        assert adapter_module._read_pending_journal(str(root / "pending_notifications.json")) == []
        assert restarted._journal_notifications == {}


def test_failed_turn_keeps_one_retry_without_duplicate_queue_entries() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        adapter = _adapter(root)
        asyncio.run(adapter._queue_mail_notification(_event(), event_id="evt-1"))
        adapter._pending_notifications.clear()
        batch = MessageEvent(
            raw_message={"event_type": "openmail.mailbox.batch", "event_ids": ["evt-1"]},
            source=SimpleNamespace(chat_id="mailbox"),
        )

        asyncio.run(adapter.on_processing_complete(batch, ProcessingOutcome.FAILURE))
        asyncio.run(adapter.on_processing_complete(batch, ProcessingOutcome.FAILURE))

        assert [item["event_id"] for item in adapter._pending_notifications] == ["evt-1"]
        assert [item["event_id"] for item in adapter_module._read_pending_journal(adapter._pending_journal_path)] == ["evt-1"]


def test_corrupt_pending_journal_is_not_treated_as_empty() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "pending_notifications.json"
        path.write_text("not json\n", encoding="utf-8")
        try:
            adapter_module._read_pending_journal(str(path))
        except json.JSONDecodeError:
            pass
        else:
            raise AssertionError("corrupt pending journal must fail closed")
