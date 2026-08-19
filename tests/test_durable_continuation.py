"""Legacy-state migration coverage for the REST-only poller.

The detailed checkpoint, persistence, retry, readiness, and pagination contract is
covered in test_rest_poller.py. This file preserves the one transition concern
that matters to already deployed v1 agents.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import tempfile
import types
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


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

        def _set_fatal_error(self, *args, **kwargs):
            self._running = False

        async def _notify_fatal_error(self):
            return None

        def _mark_connected(self):
            self._running = True

        def _mark_disconnected(self):
            self._running = False

    setattr(gateway_config, "Platform", Platform)
    setattr(gateway_config, "PlatformConfig", PlatformConfig)
    setattr(gateway_base, "BasePlatformAdapter", BasePlatformAdapter)
    setattr(gateway_base, "MessageEvent", MessageEvent)
    setattr(gateway_base, "MessageType", MessageType)
    setattr(gateway_base, "ProcessingOutcome", ProcessingOutcome)
    setattr(gateway_base, "SendResult", SendResult)
    setattr(gateway_base, "build_session_key", lambda source, **kwargs: "mailbox")
    sys.modules["gateway"] = gateway
    sys.modules["gateway.config"] = gateway_config
    sys.modules["gateway.platforms"] = gateway_platforms
    sys.modules["gateway.platforms.base"] = gateway_base

    spec = importlib.util.spec_from_file_location(
        "openmail_legacy_migration_adapter_under_test", ROOT / "adapter.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, PlatformConfig


adapter_module, PlatformConfig = _load_adapter_module()


def test_legacy_pending_row_migrates_into_atomic_state_and_is_retired() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy = root / "pending_notifications.json"
            state = root / "poller_state.json"
            legacy.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "notifications": [
                            {
                                "event_id": "event-old",
                                "message_id": "message-old",
                                "inbox_id": "inbox-1",
                                "thread_id": "thread-1",
                                "from": "sender@example.invalid",
                                "subject": "Pending",
                                "timestamp": "2026-08-19T00:00:00Z",
                                "raw": {
                                    "message": {
                                        "id": "message-old",
                                        "body_text": "must not migrate",
                                    }
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            config = PlatformConfig(
                extra={
                    "api_key": "test-key",
                    "inbox_ids": ["inbox-1"],
                    "api_base_url": "https://api.openmail.invalid",
                    "poll_state_path": str(state),
                    "legacy_pending_journal_path": str(legacy),
                    "notification_batch_window_seconds": 0,
                    "notification_min_interval_seconds": 0,
                }
            )
            adapter = adapter_module.OpenMailMailboxAdapter(config)
            adapter._ensure_flush_task_locked = lambda: None

            async def summaries(_inbox_id: str):
                return {
                    "thread-1": {
                        "id": "thread-1",
                        "messageCount": 1,
                        "lastMessageAt": "2026-08-19T00:00:00Z",
                    }
                }

            async def messages(_inbox_id: str):
                return [
                    {
                        "id": "message-old",
                        "threadId": "thread-1",
                        "inboxId": "inbox-1",
                        "direction": "inbound",
                        "createdAt": "2026-08-19T00:00:00Z",
                    }
                ]

            adapter._fetch_all_thread_summaries = summaries
            adapter._fetch_all_inbound_messages = messages
            await adapter._load_state_for_poll()
            assert await adapter._poll_once() == 0

            payload = json.loads(state.read_text(encoding="utf-8"))
            assert list(payload["pending"]) == ["message-old"]
            assert "must not migrate" not in state.read_text(encoding="utf-8")
            assert not legacy.exists()

    asyncio.run(scenario())


def test_corrupt_legacy_pending_state_fails_closed() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy = root / "pending_notifications.json"
            legacy.write_text("not json\n", encoding="utf-8")
            config = PlatformConfig(
                extra={
                    "api_key": "test-key",
                    "inbox_ids": ["inbox-1"],
                    "poll_state_path": str(root / "poller_state.json"),
                    "legacy_pending_journal_path": str(legacy),
                }
            )
            adapter = adapter_module.OpenMailMailboxAdapter(config)
            try:
                await adapter._load_state_for_poll()
            except json.JSONDecodeError:
                return
            raise AssertionError("corrupt legacy state must not be treated as empty")

    asyncio.run(scenario())
