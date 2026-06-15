# OpenMail Mailbox Hermes plugin

Outbound-only OpenMail mailbox platform adapter for Hermes.

The plugin opens an outbound WebSocket to OpenMail at gateway startup, subscribes to `message.received` events for the configured inbox, and routes every notification to one stable Hermes mailbox session. The notification includes metadata only; the agent should inspect/reply/send through the OpenMail CLI using the `openmail` skill.

## Native install

```bash
hermes plugins install dreamcatcher-agents/openmail-mailbox --enable
```

Configure `OPENMAIL_API_KEY`, `OPENMAIL_INBOX_ID`, and `OPENMAIL_ADDRESS` in `/opt/data/.env.work` (or the target Hermes runtime environment) and set `platforms.openmail_mailbox.extra.inbox_ids`/`address` in `config.yaml`, then restart the gateway. The adapter reads `/opt/data/.env.work` directly so OpenMail secrets do not need to be copied into `config.yaml`.

## Runtime shape

- Plugin name: `openmail-mailbox`
- Platform name: `openmail_mailbox`
- Required secret: `OPENMAIL_API_KEY`
- Required inbox selector: `OPENMAIL_INBOX_ID` or platform `extra.inbox_ids`
- Optional address: `OPENMAIL_ADDRESS` or platform `extra.address`
- Default session id: `openmail-mailbox:<address-local-part>` when address is known
- Watched event classes: `message.received`
- Auto skill: `openmail`

## Verification

Healthy startup logs include:

```text
Connecting outbound WebSocket to OpenMail
Started outbound OpenMail WebSocket task for 1 inbox(es)
Subscribed to OpenMail events
```

A later inbound message should produce a dispatch log and an `inbound message: platform=openmail_mailbox` gateway entry.
