import asyncio
import decimal
import enum
import json
import logging
import numbers
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from conftest import UnwritableStorage, eventually, make_client
from wbskt import Command, CredentialsNotSaved, CredentialsRejected, MemoryStorage, Parameter, RegistrationRequired, WbsktError, protocol
from wbskt.models import DataType


async def test_registers_once_and_stores_the_credentials(api, socket_server, no_jitter):
    storage = MemoryStorage()
    client = make_client(api, socket_server, no_jitter, storage=storage)
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert api.registrations == [{"pin": "483920", "name": "test-client"}]
    assert storage.load()[0] is not None

    second = make_client(api, socket_server, no_jitter, storage=storage)
    await second.start()
    await socket_server.wait_connections(2)
    await second.stop()

    assert len(api.registrations) == 1


async def test_keeps_the_credentials_while_approval_is_pending(api, socket_server, no_jitter):
    api.status = protocol.PENDING
    api.login_answers = [(401, {"code": "CLIENT_NOT_REGISTERED"}, {})]
    storage = MemoryStorage()
    client = make_client(api, socket_server, no_jitter, storage=storage)
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert len(api.registrations) == 1
    assert len(api.logins) == 2


async def test_reuses_the_token_across_reconnects(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    await client.start()
    first = await socket_server.wait_connections(1)
    await first.close()
    await socket_server.wait_connections(2)
    await client.stop()

    assert len(api.logins) == 1
    assert socket_server.tokens == ["token-1", "token-1"]


async def test_logs_in_again_when_the_socket_refuses_the_token(api, socket_server, no_jitter):
    socket_server.rejected_tokens.add("token-1")
    client = make_client(api, socket_server, no_jitter)
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert socket_server.tokens == ["token-1", "token-2"]


async def test_stops_and_keeps_the_credentials_when_they_are_no_longer_valid(api, socket_server, no_jitter):
    storage = MemoryStorage("old-ref", "old-secret")
    api.login_answers = [(401, {"code": "CLIENT_UNAUTHORIZED"}, {})]
    client = make_client(api, socket_server, no_jitter, storage=storage)
    await client.start()

    with pytest.raises(CredentialsRejected, match="old-ref"):
        await client.wait_closed()
    assert api.registrations == []
    assert len(api.logins) == 1
    assert storage.load() == ("old-ref", "old-secret")


async def test_a_wrong_pin_stops_the_client_with_the_servers_reason(api, socket_server, no_jitter):
    api.register_answers = [(404, {"code": "POLICY_NOT_FOUND", "detail": "Invalid registration PIN."}, {})]
    client = make_client(api, socket_server, no_jitter)
    await client.start()

    with pytest.raises(WbsktError, match="404 POLICY_NOT_FOUND: Invalid registration PIN"):
        await client.wait_closed()
    assert len(api.registrations) == 1


async def test_a_refusal_is_a_warning_once_and_then_info(api, socket_server, no_jitter, caplog):
    refusal = (401, {"code": "CLIENT_NOT_REGISTERED", "detail": "Client registration is Pending. Access denied."}, {})
    api.login_answers = [refusal, refusal]
    client = make_client(api, socket_server, no_jitter, storage=MemoryStorage("ref", "secret"))
    with caplog.at_level(logging.INFO, logger="wbskt"):
        await client.start()
        await socket_server.wait_connections(1, timeout=15)
        await client.stop()

    failures = [r for r in caplog.records if "connect failed" in r.getMessage()]
    assert [r.levelno for r in failures] == [logging.WARNING, logging.INFO]
    assert "CLIENT_NOT_REGISTERED: Client registration is Pending" in failures[0].getMessage()


async def test_a_failed_save_stops_the_client_instead_of_registering_again(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter, storage=UnwritableStorage())
    await client.start()

    with pytest.raises(WbsktError, match="can't save its credentials"):
        await client.wait_closed()
    assert len(api.registrations) == 1


async def test_credentials_kept_in_memory_survive_a_failed_login(api, socket_server, no_jitter):
    storage = UnwritableStorage()
    client = make_client(api, socket_server, no_jitter, storage=storage)
    client._credentials = ("ref", "secret")
    api.login_answers = [(503, {}, {})]
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert api.registrations == []
    assert [login["clientRefId"] for login in api.logins] == ["ref", "ref"]


@pytest.mark.parametrize("restart_with_stop", [False, True])
async def test_a_secret_saved_after_a_refusal_is_used_by_the_next_start(api, socket_server, no_jitter, restart_with_stop):
    storage = MemoryStorage("ref", "old-secret")
    api.login_answers = [(401, {"code": "CLIENT_UNAUTHORIZED"}, {})]
    client = make_client(api, socket_server, no_jitter, storage=storage)
    await client.start()
    with pytest.raises(CredentialsRejected):
        await client.wait_closed()

    storage.save("ref", "new-secret")
    if restart_with_stop:
        await client.stop()
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert [login["secret"] for login in api.logins] == ["old-secret", "new-secret"]
    assert api.registrations == []


async def test_credentials_cleared_after_a_refusal_register_again_on_the_next_start(api, socket_server, no_jitter):
    storage = MemoryStorage("ref", "old-secret")
    api.login_answers = [(401, {"code": "CLIENT_UNAUTHORIZED"}, {})]
    client = make_client(api, socket_server, no_jitter, storage=storage)
    await client.start()
    with pytest.raises(CredentialsRejected):
        await client.wait_closed()

    storage.clear()
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert len(api.registrations) == 1
    assert storage.load()[1] == "secret-1"


async def test_credentials_not_saved_are_saved_before_the_next_start_logs_in(api, socket_server, no_jitter):
    storage = UnwritableStorage()
    client = make_client(api, socket_server, no_jitter, storage=storage)
    await client.start()
    with pytest.raises(CredentialsNotSaved) as refused:
        await client.wait_closed()

    # Still read-only: refused again, and nothing logs in with credentials only this process holds.
    await client.start()
    with pytest.raises(CredentialsNotSaved):
        await client.wait_closed()
    assert api.logins == []

    storage.writable = True
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert storage.load() == (refused.value.client_ref_id, "secret-1")
    assert len(api.registrations) == 1


@pytest.mark.parametrize("name", ["", "   ", "x" * 101, " " + "x" * 101])
def test_a_name_the_platform_cannot_store_is_refused_when_the_client_is_built(name):
    from wbskt import AsyncWbsktClient, WbsktClient

    with pytest.raises(ValueError, match="1-100 characters"):
        AsyncWbsktClient(name, "483920", storage=MemoryStorage())
    with pytest.raises(ValueError, match="1-100 characters"):
        WbsktClient(name, "483920", storage=MemoryStorage())


def test_a_name_of_100_characters_once_trimmed_is_accepted():
    from wbskt import AsyncWbsktClient

    AsyncWbsktClient(" " + "x" * 100 + "\n", "483920", storage=MemoryStorage())


async def test_an_invalid_name_from_the_server_stops_the_client(api, socket_server, no_jitter):
    api.register_answers = [(400, {"code": "CLIENT_NAME_INVALID", "detail": "Client name must be 1-100 characters."}, {})]
    client = make_client(api, socket_server, no_jitter)
    await client.start()

    with pytest.raises(WbsktError, match="400 CLIENT_NAME_INVALID"):
        await client.wait_closed()
    assert len(api.registrations) == 1


async def test_waits_for_retry_after_when_throttled(api, socket_server, no_jitter):
    api.login_answers = [(429, {}, {"Retry-After": "0.3"})]
    client = make_client(api, socket_server, no_jitter)
    started = asyncio.get_running_loop().time()
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert asyncio.get_running_loop().time() - started >= 0.3


async def test_a_throttle_without_retry_after_backs_off_like_a_failure(api, socket_server, no_jitter, monkeypatch):
    from wbskt import reconnect

    # No jitter makes every back-off zero, so count the failures the policy is asked about instead.
    failures = []
    retry_delay = reconnect.ReconnectPolicy.retry_delay
    monkeypatch.setattr(reconnect.ReconnectPolicy, "retry_delay", lambda self, n: failures.append(n) or retry_delay(self, n))
    api.login_answers = [(429, {}, {}), (429, {}, {"Retry-After": "soon"})]
    client = make_client(api, socket_server, no_jitter)
    await client.start()
    await socket_server.wait_connections(1)
    await client.stop()

    assert failures == [0, 1]


async def test_a_refused_registration_is_a_warning_naming_the_code(api, socket_server, no_jitter, caplog):
    # A full plan answers 409; logged at INFO, the board looked merely "not connecting" (wbskt/Wbskt#988).
    api.register_answers = [(409, {"code": "PLAN_CLIENT_LIMIT_REACHED", "detail": "You have 20 clients; Free allows 20."}, {})]
    client = make_client(api, socket_server, no_jitter)
    with caplog.at_level(logging.WARNING, logger="wbskt"):
        await client.start()
        await socket_server.wait_connections(1)
        await client.stop()

    assert len(api.registrations) == 2
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("409 PLAN_CLIENT_LIMIT_REACHED" in m and "Free allows 20" in m for m in warnings)


async def test_stops_without_a_pin_or_credentials(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    client.policy_pin = None
    await client.start()

    with pytest.raises(RegistrationRequired):
        await asyncio.wait_for(client.wait_closed(), 5)
    await client.stop()


async def test_sends_capabilities_first_then_the_backlog_in_order(api, socket_server, no_jitter):
    relay = Command("relay.set", "Switch the relay", [Parameter("on", DataType.BOOLEAN, is_required=True)])
    client = make_client(api, socket_server, no_jitter, capabilities=[relay])
    await client.send("temperature", {"celsius": 20})
    await client.send("temperature", {"celsius": 21})
    await client.start()

    capabilities = await socket_server.next()
    first = await socket_server.next()
    second = await socket_server.next()
    await client.stop()

    assert capabilities["type"] == protocol.CAPABILITIES
    assert capabilities["payload"]["agent"] == "wbskt-python"
    assert capabilities["payload"]["capabilities"] == [
        {
            "command": "relay.set",
            "description": "Switch the relay",
            "parameters": [{"name": "on", "label": "on", "dataType": "boolean", "description": "", "isRequired": True}],
        }
    ]
    assert [first["payload"], second["payload"]] == [{"celsius": 20}, {"celsius": 21}]
    assert first["sentAt"] <= second["sentAt"]


async def test_connected_once_capabilities_are_out_while_the_backlog_drains(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    connected_with_backlog = []
    client.on_connected = lambda: connected_with_backlog.append(len(client._buffer))
    # More than the burst, so the backlog takes paced time to drain.
    for n in range(protocol.SEND_BURST + 5):
        await client.send("count", n)
    await client.start()
    await client.wait_first_attempt()

    assert client.connected
    await eventually(lambda: connected_with_backlog)
    assert connected_with_backlog[0] > 0
    await client.stop()


async def test_capabilities_set_while_the_backlog_drains_reach_the_server(api, socket_server, no_jitter):
    relay = Command("relay.set", "Switch the relay")
    client = make_client(api, socket_server, no_jitter)
    for n in range(protocol.SEND_BURST + 5):
        await client.send("count", n)
    await client.start()
    await client.wait_first_attempt()
    await client.set_capabilities([relay])

    frames = [await socket_server.next() for _ in range(protocol.SEND_BURST + 7)]
    await client.stop()

    capabilities = [f["payload"]["capabilities"] for f in frames if f["type"] == protocol.CAPABILITIES]
    assert capabilities[0] == []
    assert [c["command"] for c in capabilities[1]] == ["relay.set"]


async def test_a_send_from_on_connected_follows_the_backlog(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)

    async def hello() -> None:
        await client.send("hello")

    client.on_connected = hello
    await client.send("count", 1)
    await client.start()

    await socket_server.next()
    sent = [(await socket_server.next())["type"], (await socket_server.next())["type"]]
    await client.stop()

    assert sent == ["count", "hello"]


async def test_capabilities_carry_the_firmware_version_only_when_given(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter, firmware_version="2.4.1")
    await client.start()
    capabilities = await socket_server.next()
    await client.stop()

    assert capabilities["payload"]["firmwareVersion"] == "2.4.1"

    client = make_client(api, socket_server, no_jitter)
    await client.start()
    capabilities = await socket_server.next()
    await client.stop()

    assert "firmwareVersion" not in capabilities["payload"]


async def test_buffer_drops_the_oldest_when_full(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter, offline_buffer_size=2)
    for n in range(3):
        await client.send("count", n)
    await client.start()

    await socket_server.next()
    sent = [(await socket_server.next())["payload"], (await socket_server.next())["payload"]]
    await client.stop()

    assert sent == [1, 2]


async def test_answers_a_ping_with_a_pong(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    await client.start()
    ws = await socket_server.wait_connections(1)
    await socket_server.next()
    await ws.send('{"type":"sys.ping","payload":{"timestamp":"2026-10-31T12:00:00.1234567Z","id":"p1"}}')

    pong = await socket_server.next()
    await client.stop()

    assert pong == {"type": "sys.pong", "payload": {"originalTimestamp": "2026-10-31T12:00:00.1234567Z", "id": "p1"}}


async def test_acks_a_command_before_handing_it_over(api, socket_server, no_jitter):
    received = []
    client = make_client(api, socket_server, no_jitter)
    client.on_message = received.append
    await client.start()
    ws = await socket_server.wait_connections(1)
    await socket_server.next()
    later = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()
    await ws.send(f'{{"type":"relay.set","payload":{{"on":true}},"commandId":"c1","expiresAt":"{later}"}}')

    ack = await socket_server.next()
    await eventually(lambda: received)
    await client.stop()

    assert ack == {"type": "sys.ack", "payload": {"commandId": "c1"}}
    assert received[0].type == "relay.set"
    assert received[0].payload == {"on": True}
    assert received[0].command_id == "c1"


async def test_refuses_an_expired_command(api, socket_server, no_jitter):
    received = []
    client = make_client(api, socket_server, no_jitter)
    client.on_message = received.append
    await client.start()
    ws = await socket_server.wait_connections(1)
    await socket_server.next()
    await ws.send('{"type":"relay.set","payload":{},"commandId":"c2","expiresAt":"2020-01-01T00:00:00Z"}')

    ack = await socket_server.next()
    await client.stop()

    assert ack == {"type": "sys.ack", "payload": {"commandId": "c2", "type": "relay.set", "refused": "expired"}}
    assert received == []


async def test_async_handlers_get_messages_in_order(api, socket_server, no_jitter):
    received = []

    async def handle(message):
        await asyncio.sleep(0.01)
        received.append(message.payload)

    client = make_client(api, socket_server, no_jitter)
    client.on_message = handle
    await client.start()
    ws = await socket_server.wait_connections(1)
    for n in range(5):
        await ws.send(f'{{"type":"tick","payload":{n}}}')
    await eventually(lambda: len(received) == 5)
    await client.stop()

    assert received == [0, 1, 2, 3, 4]


@pytest.mark.parametrize("message_type", ["sys.custom", "SYS.ping", "capabilities", "State.Report", "", "x" * 101])
async def test_rejects_a_type_it_may_not_send(api, socket_server, no_jitter, message_type):
    client = make_client(api, socket_server, no_jitter)

    with pytest.raises(ValueError):
        await client.send(message_type, {})


async def test_rejects_a_message_over_the_limit(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)

    with pytest.raises(ValueError):
        await client.send("blob", "x" * protocol.MAX_MESSAGE_BYTES)


async def test_reports_state(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    await client.start()
    await socket_server.wait_connections(1)
    await socket_server.next()
    await eventually(lambda: client.connected)
    await client.report_state({"humidity": 40})

    report = await socket_server.next()
    await client.stop()

    assert report["type"] == "state.report"
    assert report["payload"] == {"humidity": 40}


async def test_a_revoked_close_logs_in_again_before_reconnecting(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    await client.start()
    first = await socket_server.wait_connections(1)
    await first.close(code=protocol.CLOSE_REVOKED, reason="revoked")

    await socket_server.wait_connections(2)
    await client.stop()

    assert len(api.logins) == 2


async def test_a_raising_on_disconnected_still_reconnects_by_the_close_code(api, socket_server, no_jitter, caplog):
    client = make_client(api, socket_server, no_jitter)

    def boom():
        raise RuntimeError("handler failed")

    client.on_disconnected = boom
    await client.start()
    first = await socket_server.wait_connections(1)
    await first.close(code=protocol.CLOSE_REVOKED, reason="revoked")

    await socket_server.wait_connections(2)
    await client.stop()

    # The revoked close was still read, so the client logged in again.
    assert len(api.logins) == 2
    assert "on_disconnected raised" in caplog.text


async def test_a_superseded_close_keeps_the_token(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    await client.start()
    first = await socket_server.wait_connections(1)
    await first.close(code=protocol.CLOSE_SUPERSEDED, reason="superseded")

    await socket_server.wait_connections(2)
    await client.stop()

    assert len(api.logins) == 1


async def test_reconnects_quickly_after_a_restart_close(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    await client.start()
    first = await socket_server.wait_connections(1)
    await first.close(code=protocol.CLOSE_SERVER_RESTARTING, reason="restarting")

    await socket_server.wait_connections(2)
    await client.stop()


async def test_a_nan_or_infinite_reading_is_refused_rather_than_sent_as_invalid_json(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)

    with pytest.raises(ValueError, match="JSON"):
        await client.report_state({"humidity": 41, "temp": float("nan")})
    with pytest.raises(ValueError, match="JSON"):
        await client.send("reading", {"value": float("inf")})
    assert len(client._buffer) == 0


async def test_numbers_of_other_types_are_sent_as_numbers(api, socket_server, no_jitter):
    class Mode(enum.Enum):
        AUTO = "auto"

    client = make_client(api, socket_server, no_jitter)
    await client.report_state({
        "count": _FakeInt(3),
        "price": decimal.Decimal("1.25"),
        "at": datetime(2026, 11, 19, 7, 0, tzinfo=timezone.utc),
        "id": uuid.UUID(int=1),
        "mode": Mode.AUTO,
    })

    payload = json.loads(client._buffer[0])["payload"]
    assert payload == {"count": 3, "price": 1.25, "at": "2026-11-19T07:00:00+00:00", "id": str(uuid.UUID(int=1)), "mode": "auto"}

    with pytest.raises(ValueError, match="object is not JSON serializable"):
        await client.send("reading", {"thing": object()})


async def test_non_ascii_text_is_measured_in_utf8_bytes(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    # 20,000 CJK characters: 60 KB as UTF-8, but 120 KB as \u escapes.
    await client.send("note", "漢" * 20000)

    assert "漢" in client._buffer[0]


class _FakeInt:
    """Stands in for numpy.int64: an Integral that json can't write on its own."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __int__(self) -> int:
        return self.value

    def __index__(self) -> int:
        return self.value


numbers.Integral.register(_FakeInt)


async def test_flush_waits_for_the_backlog_to_go_out(api, socket_server, no_jitter, caplog):
    client = make_client(api, socket_server, no_jitter)
    await client.send("temperature", {"celsius": 20})
    assert await client.flush(0.1) is False  # not started: nothing can leave

    await client.start()
    assert await client.flush(5) is True
    await client.stop()

    assert (await socket_server.next())["type"] == protocol.CAPABILITIES
    assert (await socket_server.next())["payload"] == {"celsius": 20}
    assert "unsent" not in caplog.text


async def test_flush_gives_up_while_offline_and_stop_says_what_was_left(api, no_jitter, caplog):
    from wbskt import AsyncWbsktClient, MemoryStorage

    client = AsyncWbsktClient("test-client", "483920", api_url=api.url, socket_url="ws://127.0.0.1:1",
                              storage=MemoryStorage(), reconnect_policy=no_jitter)
    await client.start()
    await client.wait_first_attempt()
    await client.send("temperature", {"celsius": 20})
    await client.send("temperature", {"celsius": 21})

    assert await client.flush(0.2) is False
    with caplog.at_level(logging.WARNING, logger="wbskt"):
        await client.stop(flush_timeout=0.1)

    assert "stopped with 2 queued message(s) unsent" in caplog.text


async def test_stop_with_a_flush_timeout_sends_the_backlog_first(api, socket_server, no_jitter):
    client = make_client(api, socket_server, no_jitter)
    await client.send("temperature", {"celsius": 20})
    await client.start()
    await client.stop(flush_timeout=5)

    assert (await socket_server.next())["type"] == protocol.CAPABILITIES
    assert (await socket_server.next())["payload"] == {"celsius": 20}


async def test_flush_without_a_timeout_returns_when_the_client_stops_for_good(api, socket_server, no_jitter):
    from wbskt import AsyncWbsktClient, MemoryStorage

    # No credentials and no PIN: the client stops with RegistrationRequired and can never drain.
    client = AsyncWbsktClient("test-client", None, api_url=api.url, socket_url=socket_server.url,
                              storage=MemoryStorage(), reconnect_policy=no_jitter)
    await client.send("temperature", {"celsius": 20})
    await client.start()

    assert await asyncio.wait_for(client.flush(), 5) is False
    await client.stop()
