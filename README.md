# OpenMail Mailbox Hermes plugin

Outbound-only, REST-only OpenMail mailbox poller for Hermes.

The plugin completes a provider reconciliation before declaring itself ready, then polls OpenMail's lightweight thread index every two minutes by default. It fetches message details only for new or changed threads and routes new inbound-message metadata to one stable Hermes mailbox session. Message bodies are never stored in poll state; the agent inspects and replies through the OpenMail CLI using the bundled `openmail` skill.

## Native install

```bash
hermes plugins install dreamcatcher-agents/openmail-mailbox --enable
```

## Outbound CLI dependency

This plugin owns inbound REST polling only. It deliberately does not install or update the external `@openmail/cli` package used to read, send, and reply to mail. A managed Hermes runtime must provide a tested `openmail` command on `PATH` and should pin the CLI and applied skill together in its image/release contract. Updating this plugin alone does not repair a missing CLI command.

Do not use an unversioned `npx @openmail/cli` invocation as the managed-fleet runtime path: it can depend on network availability or reuse a stale npm cache. Promote CLI changes through the owning runtime image, then verify the command, version, supported help surface, and a mailbox smoke before fleet rollout.

Configure `OPENMAIL_API_KEY`, `OPENMAIL_INBOX_ID`, and `OPENMAIL_ADDRESS` in `/opt/data/.env.work` (or the target Hermes runtime environment), then restart the gateway. The adapter reads `/opt/data/.env.work` directly so OpenMail secrets do not need to be copied into `config.yaml`.

## Runtime shape

- Plugin name: `openmail-mailbox`
- Platform name: `openmail_mailbox`
- Required secret: `OPENMAIL_API_KEY`
- Required inbox selector: `OPENMAIL_INBOX_ID` or platform `extra.inbox_ids`
- Optional address: `OPENMAIL_ADDRESS` or platform `extra.address`
- Optional API root: `OPENMAIL_API_BASE_URL` (default `https://api.openmail.sh`)
- Default poll interval: 120 seconds plus 0–15 seconds of jitter
- Default session id: `openmail-mailbox:<address-local-part>` when address is known
- Auto skill: `openmail`
- Platform lifecycle: accepts Hermes `connect(*, is_reconnect=False)` startup/reconnect calls
- Atomic state: `/opt/data/openmail-mailbox/poller_state.json`

## Reliability and state

The provider mailbox is authoritative. On first boot the adapter brackets a complete inbound-message ID scan with two thread-index snapshots and accepts the baseline only when both snapshots match. Existing mail is baselined rather than dispatched.

REST response bodies are read through end-of-stream in bounded chunks and rejected when they exceed the endpoint-specific cap. This keeps large or transfer-chunked mailbox reconciliations complete without turning the safety bound into an unbounded read.

Later polls fetch only new or changed threads. Before dispatch, one owner-only atomic state file records both:

1. Per-thread message count, last-message timestamp, and known inbound message IDs.
2. Minimal redacted metadata for messages whose agent turn has not completed successfully.

There is no separate provider cursor or active pending journal. A legacy v1 pending journal is imported only when no v2 poll state exists, then retired after the first successful baseline. Corrupt state fails closed instead of being treated as an empty mailbox.

A failed or cancelled turn retains exactly one durable retry. A successful turn removes its pending message IDs atomically. Because an external email side effect cannot be transacted with local acknowledgement, a crash after sending but before acknowledgement can replay a notification; the mailbox prompt therefore requires inspecting current thread state before sending.

## Verification

Healthy startup logs include:

```text
Established stable OpenMail REST baseline
REST-only OpenMail poller ready after initial reconciliation
```

A later inbound message should produce:

```text
Durably discovered 1 new inbound OpenMail message(s) before dispatch
Dispatching REST-discovered OpenMail batch
```

and an `inbound message: platform=openmail_mailbox` gateway entry. A poll failure is reported to the gateway reconnect supervisor rather than leaving a falsely healthy watcher.
