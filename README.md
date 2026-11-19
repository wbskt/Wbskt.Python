<img src="https://raw.githubusercontent.com/wbskt/Wbskt.Python/master/assets/icon.png" alt="" width="64" height="64">

# wbskt

Connect a Raspberry Pi, a script or a service to [Wbskt](https://wbskt.com). The client registers
once with a policy PIN, holds a WebSocket open, sends readings and receives commands. It comes back
on its own after a drop.

```sh
pip install wbskt
```

Python 3.10 or later. The only dependency is `websockets`. This is a 0.9 beta: the client protocol
it speaks may still change before 1.0.

## Quick start

```python
import time
from wbskt import WbsktClient

def read_sensor():
    return 21.5

client = WbsktClient("greenhouse-sensor", policy_pin="483920")
client.on_message = lambda message: print("got", message.type, message.payload)
client.start()

while True:
    client.send("temperature", {"celsius": read_sensor()})
    time.sleep(10)
```

Get the PIN from a registration policy in the console (**Policies**). The first run registers the
client and stores its credentials in your user data folder (`~/.local/share/wbskt/<name>.json` on
Linux), so later runs sign in without registering again. If the policy needs approval, the client
waits and connects once someone approves it in the console. That folder must be writable: if the
credentials can't be saved, or a credentials file is there but can't be read, the client stops with
a `WbsktError` naming the path rather than registering a second client. Under a read-only root or a
sandboxed service, pass `storage=FileStorage(path)` with a writable path.

`start()` returns once the first connection attempt has finished; a failed one is retried in the
background. The client runs on a daemon thread, so it stops when your script ends: keep the script
running for as long as the client should stay connected. A script with nothing else to do calls
`client.run_forever()` instead of `start()`, which blocks until Ctrl+C.

## asyncio

```python
import asyncio
from wbskt import AsyncWbsktClient

async def main():
    client = AsyncWbsktClient("greenhouse-sensor", policy_pin="483920")

    async def handle(message):
        print(message.type, message.payload)

    client.on_message = handle
    await client.start()
    await client.wait_first_attempt()  # optional: start() only begins connecting
    while True:
        await client.send("temperature", {"celsius": read_sensor()})
        await asyncio.sleep(10)

asyncio.run(main())
```

Both clients take the same options.

## Commands

Tell the console which commands the client takes, and it offers them with a form:

```python
from wbskt import Command, DataType, Parameter, WbsktClient

relay = Command("relay.set", "Switch the relay", [Parameter("on", DataType.BOOLEAN, is_required=True)])
client = WbsktClient("greenhouse-relay", policy_pin="483920", capabilities=[relay])

def handle(message):
    if message.type == "relay.set":
        set_relay(message.payload["on"])
        client.send("relay.state", {"on": message.payload["on"]})

client.on_message = handle
```

The SDK confirms delivery of a command as soon as it arrives, before your handler runs, and refuses
one that arrives after its expiry without calling your handler. Report the outcome as a message of
your own if it matters. Call `set_capabilities(...)` when the set of commands changes.

## Version

Pass `firmware_version="2.4.1"` to say which version of your own code the client runs. The console
shows it on the client page. It is not the SDK's version, which is sent for you.

## State

```python
client.report_state({"humidity": 41, "fan": "on"})
```

Send only what changed. Numbers are kept as readings and charted in the console; numpy scalars and
`Decimal` go out as numbers too. NaN and infinity aren't JSON, so `report_state` and `send` raise
`ValueError` for them: leave out a sensor that failed to read (a DHT22 returns NaN) or send `None`.
`datetime`, `UUID` and enum values go out as text; anything else JSON can't hold raises `ValueError`.

## While offline

`send` never blocks on the network. While disconnected, messages wait in memory (the last 1,000 by
default, `offline_buffer_size=`) and go out in order, with the time they were made, once the
connection is back. Workflows can tell those late readings from live ones.

## Secret rotated or client deleted

When someone rotates the client's secret or deletes the client in the console, login refuses the
stored credentials. The client then stops with `CredentialsRejected` (logged on the `wbskt` logger and
raised from `start()`, `wait_closed()` and `run_forever()`) and keeps its credentials file. It never
registers again by itself: a deleted device would come back as a new client, and a rotated one would
stay offline beside a duplicate.

- **Secret rotated:** save the new secret from the console beside the same client id, then start again.

  ```python
  from wbskt import FileStorage

  storage = FileStorage.for_client("greenhouse-sensor")
  client_ref_id, _ = storage.load()
  storage.save(client_ref_id, "the-new-secret")
  ```

- **Register again** (the client was deleted, or you point it at a fresh install): delete the
  credentials file (`FileStorage.for_client("greenhouse-sensor").clear()`). The next start registers
  with the PIN as a new client.

## Options

| Option | Default | |
|---|---|---|
| `api_url` | `https://api.dev.wbskt.com` | A self-hosted install's `https://api.<domain>`. |
| `socket_url` | `wss://ws.dev.wbskt.com` | `wss://ws.<domain>`. |
| `storage` | `FileStorage.for_client(name)` | `FileStorage(path)`, `MemoryStorage(ref, secret)`, or your own object with `load`, `save` and `clear`. |
| `capabilities` | none | The commands the client takes. |
| `offline_buffer_size` | 1000 | Messages kept while offline; the oldest go first. |

Handlers can be `on_message(message)`, `on_connected()` and `on_disconnected()`. On `WbsktClient`
they run on the client's background thread, so keep them short.

Messages are at most 64 KB, and types starting with `sys.`, `capabilities` and `state.report` are
reserved; `send` raises `ValueError` for either.

## Troubleshooting

The client logs on the `wbskt` logger. A script that sets up no logging shows only warnings and
errors, which is where every refusal goes: the first time an attempt fails a given way it is a
warning, and the same failure on the retries after it is logged at INFO. To see every attempt:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

| What the log says | Why, and what to do |
|---|---|
| `registration answered 404 POLICY_NOT_FOUND` | The PIN matches no policy. The client stops with `WbsktError`: check the PIN in the console (**Policies**). |
| `registration answered 400 REGISTRATION_PIN_REQUIRED` | The PIN was empty. The client stops with `WbsktError`. |
| `registration answered 403 POLICY_DISABLED` | The policy is turned off. Turn it on in the console; the client retries. |
| `registration answered 400 POLICY_LIMIT_REACHED` or `409 PLAN_CLIENT_LIMIT_REACHED` | The policy or the plan is full. Remove a client you no longer use; the client retries. |
| `login answered 401 CLIENT_NOT_REGISTERED` | Waiting for approval, or revoked or rejected. Approve it in the console; the client retries. |
| `CredentialsRejected` / `CLIENT_UNAUTHORIZED` | The client was deleted or its secret rotated. See [above](#secret-rotated-or-client-deleted). |
| `connect failed (<urlopen error ...>)` | No server at `api_url`, no network, or a TLS error. Check the URL; the client retries. |
| `rate limited` | Too many logins from your address. The client waits as the server asks. |
| `no stored credentials and no policy PIN` | First run without a PIN. Pass the policy PIN. |

## Protocol

The SDK implements [the client protocol](https://github.com/wbskt/Wbskt/blob/master/Docs/Client.Protocol.md)
and its [WebSocket binding](https://github.com/wbskt/Wbskt/blob/master/Docs/Client.Protocol.WebSocket.md):
token reuse, jittered reconnects that honour `Retry-After` (up to an hour; a 429 without one backs off), ping answers and command
acknowledgements. Read it to write a client in another language.
