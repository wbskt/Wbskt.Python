"""The Python client: register once, keep a token, hold the socket open, and come back after a drop.

Follows Docs/Client.Protocol.md in wbskt/Wbskt; its conformance suite (Tests/Conformance) checks this client.
"""

import asyncio
import collections
import concurrent.futures
import decimal
import enum
import inspect
import json
import logging
import math
import numbers
import platform
import re
import threading
import time
import uuid
from datetime import date, datetime, timezone
from datetime import time as dt_time
from typing import Any, Awaitable, Callable, Deque, Dict, Iterable, List, Optional, Tuple, Union

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from . import _http, protocol
from ._pacing import Pacer
from .models import Command, CredentialsRejected, Message, RegistrationRequired, WbsktError
from .reconnect import ReconnectPolicy
from .storage import FileStorage, Storage
from .version import __version__

log = logging.getLogger("wbskt")

DEFAULT_API_URL = "https://api.dev.wbskt.com"
DEFAULT_SOCKET_URL = "wss://ws.dev.wbskt.com"

TOKEN_RENEW_BEFORE = 60.0
_ISO_TIME = re.compile(r"(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<fraction>\.\d+)?(?P<zone>Z|[+-]\d{2}:\d{2})?$", re.IGNORECASE)
OFFLINE_BUFFER_SIZE = 1000
_FLUSH_POLL = 0.05

Handler = Callable[..., Union[None, Awaitable[None]]]


class _Throttled(Exception):
    def __init__(self, retry_after: Optional[float]) -> None:
        wait = "with no usable Retry-After" if retry_after is None else f"retry after {retry_after:g}s"
        super().__init__(f"rate limited, {wait}")
        self.retry_after = retry_after


class _LoginRefused(Exception):
    """Login or registration said no for now (pending approval, revoked, a full policy): retry on the schedule."""


class AsyncWbsktClient:
    """A client connection for asyncio code.

    >>> client = AsyncWbsktClient("greenhouse-sensor", policy_pin="483920")
    >>> client.on_message = handle
    >>> await client.start()
    >>> await client.send("temperature", {"celsius": 21.5})

    ``on_message(message)``, ``on_connected()`` and ``on_disconnected()`` may be plain functions or
    coroutines. Messages are handed over one at a time, in the order they arrived.
    """

    def __init__(
        self,
        client_name: str,
        policy_pin: Optional[str] = None,
        *,
        api_url: str = DEFAULT_API_URL,
        socket_url: str = DEFAULT_SOCKET_URL,
        storage: Optional[Storage] = None,
        capabilities: Iterable[Command] = (),
        offline_buffer_size: int = OFFLINE_BUFFER_SIZE,
        reconnect_policy: Optional[ReconnectPolicy] = None,
        firmware_version: Optional[str] = None,
    ) -> None:
        if not client_name:
            raise ValueError("client_name is required")
        self.client_name = client_name
        self.policy_pin = policy_pin
        self.api_url = api_url.rstrip("/")
        self.socket_url = socket_url.rstrip("/")
        self.storage: Storage = storage if storage is not None else FileStorage.for_client(client_name)
        self.on_message: Optional[Handler] = None
        self.on_connected: Optional[Handler] = None
        self.on_disconnected: Optional[Handler] = None

        self._capabilities: List[Command] = list(capabilities)
        self._firmware_version = firmware_version
        self._buffer: Deque[str] = collections.deque(maxlen=offline_buffer_size)
        self._pacer = Pacer(protocol.SEND_PER_SECOND, protocol.SEND_BURST)
        self._send_lock: Optional[asyncio.Lock] = None
        self._policy = reconnect_policy or ReconnectPolicy()
        self._token: Optional[str] = None
        self._token_expires = 0.0
        # The credentials in use, kept so a registration is never repeated because storage failed.
        self._credentials: Optional[Tuple[str, str]] = None
        self._ws: Optional[ClientConnection] = None
        self._connected = False
        # The backlog has gone out on this connection, so a send may skip the buffer. Set after
        # _connected: until then a send queues behind the backlog to keep the order it was made in.
        self._drained = False
        # Made in start(): on Python 3.9 they bind to the loop that exists when they're created.
        self._stopping: Optional[asyncio.Event] = None
        self._inbox: Optional["asyncio.Queue[Message]"] = None
        self._run_task: Optional["asyncio.Task[None]"] = None
        self._dispatch_task: Optional["asyncio.Task[None]"] = None
        self._error: Optional[BaseException] = None
        self._attempted: Optional[asyncio.Event] = None
        # The last failure logged at WARNING, so a retry that fails the same way logs at INFO instead.
        self._last_failure: Optional[str] = None

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        """Start connecting in the background. The first attempt is immediate."""
        if self._run_task is not None:
            return
        self._stopping = asyncio.Event()
        self._attempted = asyncio.Event()
        self._inbox = asyncio.Queue()
        self._send_lock = asyncio.Lock()
        self._error = None
        self._dispatch_task = asyncio.ensure_future(self._dispatch())
        self._run_task = asyncio.ensure_future(self._run())

    async def flush(self, timeout: Optional[float] = None) -> bool:
        """Wait until every queued message has gone out, or ``timeout`` seconds pass.

        Returns True when nothing is left queued. While offline the queue can't drain, so give a
        timeout unless the client is sure to connect.
        """
        if self._run_task is None or self._send_lock is None:
            return not self._buffer
        lock = self._send_lock

        async def sent() -> None:
            while True:
                if self._drained and not self._buffer:
                    # Taking the lock waits for a send already under way to finish.
                    async with lock:
                        if self._drained and not self._buffer:
                            return
                await asyncio.sleep(_FLUSH_POLL)

        try:
            await asyncio.wait_for(sent(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def stop(self, flush_timeout: Optional[float] = None) -> None:
        """Disconnect and stop. With ``flush_timeout``, first wait up to that long for queued messages.

        A warning says how many messages were still queued. They stay queued for the next ``start()``.
        """
        if flush_timeout is not None and self._run_task is not None:
            await self.flush(flush_timeout)
        if self._stopping is not None:
            self._stopping.set()
        ws = self._ws
        if ws is not None:
            await ws.close()
        for task in (self._run_task, self._dispatch_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._run_task = self._dispatch_task = None
        if self._buffer:
            log.warning("wbskt: stopped with %d queued message(s) unsent", len(self._buffer))

    async def wait_first_attempt(self) -> None:
        """Wait until the first connection attempt has finished, connected or not.

        A failed attempt is retried in the background. Only an error no retry can fix, such as
        :class:`RegistrationRequired`, is raised.
        """
        if self._attempted is not None:
            await self._attempted.wait()
        if self._error is not None:
            raise self._error

    async def wait_closed(self) -> None:
        """Wait until the client stops, raising what stopped it if it wasn't ``stop()``."""
        if self._run_task is not None:
            try:
                await asyncio.shield(self._run_task)
            except asyncio.CancelledError:
                if self._stopping is None or not self._stopping.is_set():
                    raise
        if self._error is not None:
            raise self._error

    async def send(self, message_type: str, payload: Any = None) -> None:
        """Send an application message, stamped now. Queued in order while offline."""
        await self._send_or_buffer(_message_frame(message_type, payload))

    async def report_state(self, values: Dict[str, Any]) -> None:
        """Report the state variables that changed. Numbers are kept as readings for charts."""
        await self._send_or_buffer(_state_frame(values))

    async def set_capabilities(self, capabilities: Iterable[Command]) -> None:
        """Replace the commands the client accepts, and tell the server if connected."""
        self._capabilities = list(capabilities)
        ws = self._ws
        if self._connected and ws is not None:
            try:
                await ws.send(self._capabilities_frame())
            except ConnectionClosed:
                pass

    async def _send_or_buffer(self, frame: str) -> None:
        if self._drained and self._send_lock is not None:
            # The lock is FIFO, so paced sends still leave in the order they were made.
            async with self._send_lock:
                # Read under the lock: a reconnect may have drained the backlog while this send waited,
                # and a frame buffered for the old connection would then sit until the next one.
                ws = self._ws
                if self._drained and ws is not None:
                    try:
                        await self._pacer.take()
                        await ws.send(frame)
                        return
                    except ConnectionClosed:
                        pass
                self._buffer.append(frame)
                return
        self._buffer.append(frame)

    async def _run(self) -> None:
        assert self._stopping is not None
        failures = 0
        delay = 0.0
        try:
            while not self._stopping.is_set():
                if delay > 0 and await self._sleep(delay):
                    break
                try:
                    close_code = await self._connect_once()
                    self._attempt_finished()
                    failures = 0
                    if close_code == protocol.CLOSE_REVOKED:
                        # Lost access: the token still validates until it expires, so log in afresh.
                        self._token = None
                    elif close_code == protocol.CLOSE_SUPERSEDED:
                        log.warning("wbskt: a newer connection replaced this one; if this repeats, "
                                    "another client may be using these credentials")
                    delay = self._policy.first_delay(close_code)
                except _Throttled as throttled:
                    self._attempt_finished()
                    log.warning("wbskt: %s", throttled)
                    if throttled.retry_after is not None:
                        delay = self._policy.after_throttle(throttled.retry_after)
                    else:
                        # Without a wait to honour, a 429 counts as a failure, so the back-off still
                        # grows instead of retrying a throttled login every couple of seconds.
                        delay = self._policy.retry_delay(failures)
                        failures += 1
                except WbsktError as error:
                    self._error = error
                    log.error("wbskt: %s", error)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # network, refused login, bad upgrade: all retried
                    self._attempt_finished()
                    delay = self._policy.retry_delay(failures)
                    failures += 1
                    reason = str(error) or type(error).__name__
                    # Once per distinct failure: a wrong URL or a pending approval shouldn't flood the log.
                    level = logging.INFO if reason == self._last_failure else logging.WARNING
                    self._last_failure = reason
                    log.log(level, "wbskt: connect failed (%s); retrying in %.1fs", reason, delay)
        finally:
            self._connected = self._drained = False
            self._ws = None
            self._attempt_finished()

    def _attempt_finished(self) -> None:
        if self._attempted is not None:
            self._attempted.set()

    async def _sleep(self, seconds: float) -> bool:
        """Wait, waking early on stop(). True if stopping."""
        assert self._stopping is not None
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _connect_once(self) -> Optional[int]:
        """One connection from login to close. Returns the code the server closed it with, if any."""
        token = await self._access_token()
        try:
            ws = await connect(
                f"{self.socket_url}{protocol.SOCKET_PATH}",
                additional_headers={"Authorization": f"Bearer {token}"},
                subprotocols=[protocol.SUBPROTOCOL],
                max_size=protocol.MAX_MESSAGE_BYTES,
            )
        except InvalidStatus as error:
            if error.response.status_code == 401:
                self._token = None
            raise

        self._ws = ws
        drain: Optional["asyncio.Task[None]"] = None
        try:
            sent = self._capabilities
            await ws.send(self._capabilities_frame())
            # A set_capabilities() made while that frame was in flight saw the client offline and
            # only kept the list, so it goes out now, before the client counts as connected.
            if self._capabilities is not sent:
                await ws.send(self._capabilities_frame())
            # Connected once the server knows what this client accepts, not once the backlog is
            # out: a long backlog drains at the paced rate, and the app should not wait for it.
            self._connected = True
            self._last_failure = None
            self._attempt_finished()
            log.info("wbskt: connected as %s", self.client_name)
            # The backlog drains while pings are answered.
            drain = asyncio.ensure_future(self._announce_and_drain(ws))
            async for raw in ws:
                if isinstance(raw, str):
                    await self._receive(ws, raw)
        except ConnectionClosed:
            pass
        finally:
            if drain is not None:
                drain.cancel()
            was_connected = self._connected
            self._connected = self._drained = False
            self._ws = None
            await ws.close()
            if was_connected:
                # A raising handler must not hide the close code the reconnect loop needs.
                try:
                    await _call(self.on_disconnected)
                except Exception:
                    log.exception("wbskt: on_disconnected raised")

        # Through the protocol object: Connection.close_code only arrived in websockets 13.1.
        return ws.protocol.close_code

    async def _announce_and_drain(self, ws: ClientConnection) -> None:
        """Raise on_connected, then send the offline backlog, paced, and let sends skip the buffer.

        on_connected comes first so that anything it sends queues behind the backlog, in order.
        """
        try:
            await _call(self.on_connected)
        except Exception:
            log.exception("wbskt: on_connected raised")

        assert self._send_lock is not None
        try:
            async with self._send_lock:
                while self._buffer:
                    frame = self._buffer[0]
                    await self._pacer.take()
                    await ws.send(frame)
                    # A full buffer may have dropped this frame while it was being sent.
                    if self._buffer and self._buffer[0] is frame:
                        self._buffer.popleft()
                # Nothing awaits between the empty check and this, so a send can't overtake the backlog.
                self._drained = True
        except ConnectionClosed:
            return

    async def _receive(self, ws: ClientConnection, raw: str) -> None:
        try:
            data = json.loads(raw)
            message_type = str(data["type"])
        except (ValueError, KeyError, TypeError):
            log.debug("wbskt: ignored a frame that isn't a message")
            return
        payload = data.get("payload")

        if message_type.lower() == protocol.PING:
            ping = payload if isinstance(payload, dict) else {}
            pong = {"originalTimestamp": ping.get("timestamp"), "id": ping.get("id")}
            await ws.send(_encode(protocol.PONG, pong))
            return
        if protocol.is_reserved(message_type):
            return

        command_id = data.get("commandId")
        expires_at = _parse_time(data.get("expiresAt"))
        if command_id:
            if expires_at is not None and expires_at <= datetime.now(timezone.utc):
                refusal = {"commandId": command_id, "type": message_type, "refused": protocol.REFUSED_EXPIRED}
                await ws.send(_encode(protocol.ACK, refusal))
                return
            await ws.send(_encode(protocol.ACK, {"commandId": command_id}))

        assert self._inbox is not None
        self._inbox.put_nowait(Message(message_type, payload, command_id, expires_at))

    async def _dispatch(self) -> None:
        assert self._inbox is not None
        while True:
            message = await self._inbox.get()
            try:
                await _call(self.on_message, message)
            except Exception:
                log.exception("wbskt: on_message raised")

    async def _access_token(self) -> str:
        if self._token is not None and time.monotonic() < self._token_expires - TOKEN_RENEW_BEFORE:
            return self._token

        if self._credentials is None:
            stored_ref_id, stored_secret = self.storage.load()
            if stored_ref_id and stored_secret:
                self._credentials = (stored_ref_id, stored_secret)
            else:
                self._credentials = await self._register()
        client_ref_id, secret = self._credentials

        status, body, headers = await _post(f"{self.api_url}{protocol.LOGIN_PATH}", {"clientRefId": client_ref_id, "secret": secret})
        if status == 200 and isinstance(body, dict) and body.get("accessToken"):
            self._token = str(body["accessToken"])
            self._token_expires = time.monotonic() + float(body.get("expiresIn") or 3600)
            return self._token
        if status == 429:
            raise _Throttled(_retry_after(headers))
        code = body.get("code") if isinstance(body, dict) else None
        if status == 401 and code == "CLIENT_UNAUTHORIZED":
            # Deleted, or its secret rotated. Registering again would bring a deleted device back as a
            # new client, or leave the rotated one offline beside a duplicate, so keep them and stop.
            where = getattr(self.storage, "path", type(self.storage).__name__)
            raise CredentialsRejected(
                f"login refused the credentials of {client_ref_id} (CLIENT_UNAUTHORIZED): the client was "
                f"deleted or its secret rotated in the console. If it was rotated, save the new secret to "
                f"{where} with the same client id; to register again with the PIN, remove that file"
            )
        raise _LoginRefused(_refusal("login", status, body))

    async def _register(self) -> Tuple[str, str]:
        if not self.policy_pin:
            raise RegistrationRequired("no stored credentials and no policy PIN to register with")
        status, body, headers = await _post(f"{self.api_url}{protocol.REGISTER_PATH}", {"pin": self.policy_pin, "name": self.client_name})
        if status == 429:
            raise _Throttled(_retry_after(headers))
        if status not in (200, 201) or not isinstance(body, dict) or not body.get("clientRefId"):
            code = body.get("code") if isinstance(body, dict) else None
            if code in protocol.FATAL_REGISTRATION_CODES:
                # A wrong or missing PIN: retrying with the same one can never work.
                raise WbsktError(f"{_refusal('registration', status, body)}. Check the policy PIN in the console (Policies)")
            raise _LoginRefused(_refusal("registration", status, body))

        client_ref_id, secret = str(body["clientRefId"]), str(body["secret"])
        # Kept before saving, so a failed save can't lead to a second registration in this process.
        self._credentials = (client_ref_id, secret)
        # Saved even when pending approval: registering again would make a second client.
        try:
            self.storage.save(client_ref_id, secret)
        except OSError as error:
            where = getattr(self.storage, "path", type(self.storage).__name__)
            raise WbsktError(
                f"registered as {client_ref_id} but can't save its credentials to {where}: {error}. "
                "Make it writable and start again; delete the client just registered in the console"
            ) from error
        if body.get("status") == protocol.PENDING:
            log.warning("wbskt: registered %s; waiting for approval in the console", self.client_name)
        return client_ref_id, secret

    def _capabilities_frame(self) -> str:
        frame: Dict[str, Any] = {
            "agent": "wbskt-python",
            "version": __version__,
            "os": f"{platform.system()} {platform.machine()}".strip(),
            "capabilities": [c.to_json() for c in self._capabilities],
        }
        if self._firmware_version:
            frame["firmwareVersion"] = self._firmware_version
        return _encode(protocol.CAPABILITIES, frame)


class WbsktClient:
    """The same client for code that isn't async: it runs on its own background thread.

    >>> client = WbsktClient("greenhouse-sensor", policy_pin="483920")
    >>> client.on_message = lambda message: print(message.type, message.payload)
    >>> client.start()
    >>> client.send("temperature", {"celsius": 21.5})

    The thread is a daemon, so the client stops when the script ends: keep the script running for as
    long as the client should stay connected. Handlers run on the client's thread, so keep them short
    or hand work off. A handler may call ``send``, ``report_state``, ``set_capabilities`` and ``stop``:
    on the client's own thread they are scheduled rather than waited for.
    """

    def __init__(self, client_name: str, policy_pin: Optional[str] = None, **options: Any) -> None:
        # A loop per start(): stop() closes it, with the threads its HTTP calls ran on.
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._stopped_thread: Optional[threading.Thread] = None
        self._options = (client_name, policy_pin, options)
        self._client: Optional[AsyncWbsktClient] = None
        self.on_message: Optional[Callable[[Message], None]] = None
        self.on_connected: Optional[Callable[[], None]] = None
        self.on_disconnected: Optional[Callable[[], None]] = None

    @property
    def connected(self) -> bool:
        return self._client is not None and self._client.connected

    def start(self) -> None:
        """Connect, returning once the first attempt has finished, connected or not.

        A failed attempt is retried in the background. Only an error no retry can fix, such as
        :class:`RegistrationRequired`, is raised.
        """
        if self._on_loop():
            raise RuntimeError("start() can't be called from a handler")
        if self._thread is not None:
            return
        if self._stopped_thread is not None:
            # Stopped from a handler: its loop has to finish stopping before it can run again.
            self._stopped_thread.join()
            self._stopped_thread = None
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=_run_loop, args=(self._loop,), name="wbskt", daemon=True)
        self._thread.start()
        self._client = self._wait(self._create())
        self._wait(self._client.start())
        try:
            self._wait(self._client.wait_first_attempt())
        except WbsktError:
            self.stop()
            raise

    def flush(self, timeout: Optional[float] = None) -> bool:
        """Wait until every queued message has gone out, or ``timeout`` seconds pass.

        Returns True when nothing is left queued. It can't be called from a handler, which runs on
        the thread that does the sending.
        """
        if self._on_loop():
            raise RuntimeError("flush() can't be called from a handler")
        client = self._client
        if client is None or self._thread is None:
            return True
        return bool(self._wait(client.flush(timeout)))

    def stop(self, flush_timeout: Optional[float] = None) -> None:
        """Disconnect and stop. With ``flush_timeout``, first wait up to that long for queued messages.

        Messages still queued when the client stops are discarded, and a warning says how many.
        """
        thread, client = self._thread, self._client
        if thread is None or client is None:
            return
        self._thread = None
        if self._on_loop():
            # From a handler: the loop can't wait for itself, so it stops once this handler returns.
            self._stopped_thread = thread
            loop = self._loop
            assert loop is not None
            future = asyncio.ensure_future(client.stop(flush_timeout), loop=loop)
            future.add_done_callback(lambda _: loop.stop())
            return
        loop = self._loop
        assert loop is not None
        self._wait(client.stop(flush_timeout))
        loop.call_soon_threadsafe(loop.stop)
        thread.join()

    def run_forever(self) -> None:
        """Start, then block until Ctrl+C or a fatal error such as a missing PIN."""
        if self._on_loop():
            raise RuntimeError("run_forever() can't be called from a handler")
        try:
            self.start()
            self._wait(self._require_client().wait_closed())
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def send(self, message_type: str, payload: Any = None) -> None:
        # Built here, so a bad type or an oversized message raises to the caller on any thread.
        frame = _message_frame(message_type, payload)
        self._call(self._require_client()._send_or_buffer(frame))

    def report_state(self, values: Dict[str, Any]) -> None:
        frame = _state_frame(values)
        self._call(self._require_client()._send_or_buffer(frame))

    def set_capabilities(self, capabilities: Iterable[Command]) -> None:
        self._call(self._require_client().set_capabilities(capabilities))

    async def _create(self) -> AsyncWbsktClient:
        client_name, policy_pin, options = self._options
        client = AsyncWbsktClient(client_name, policy_pin, **options)
        # Looked up on each call, so handlers can be set before or after start().
        client.on_message = lambda message: _call(self.on_message, message)
        client.on_connected = lambda: _call(self.on_connected)
        client.on_disconnected = lambda: _call(self.on_disconnected)
        return client

    def _require_client(self) -> AsyncWbsktClient:
        if self._client is None:
            raise RuntimeError("call start() first")
        if self._thread is None:
            raise RuntimeError("the client is stopped; call start() to connect again")
        return self._client

    def _call(self, coroutine: Awaitable[Any]) -> None:
        """Run a call that only queues work: waited for from another thread, scheduled from a handler."""
        if self._on_loop():
            # Waiting here would block the loop that has to run it, and the client with it.
            task = asyncio.ensure_future(coroutine, loop=self._loop)
            task.add_done_callback(_log_failure)
            return
        self._wait(coroutine)

    def _wait(self, coroutine: Awaitable[Any]) -> Any:
        if self._on_loop():
            coroutine.close()  # type: ignore[attr-defined]
            raise RuntimeError("this can't be called from a handler")
        loop = self._loop
        if loop is None or loop.is_closed():
            coroutine.close()  # type: ignore[attr-defined]
            raise RuntimeError("the client is stopped; call start() to connect again")
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)  # type: ignore[arg-type]
        # A loop that stops before it runs the call never completes it, so look up now and then
        # rather than wait for ever: a send racing stop() on another thread raises instead.
        while True:
            try:
                return future.result(timeout=0.5)
            except concurrent.futures.TimeoutError:
                if not loop.is_running() and not future.done():
                    future.cancel()
                    raise RuntimeError("the client is stopped; call start() to connect again") from None

    def _on_loop(self) -> bool:
        """Whether the caller is the client's own loop: a handler, even one that runs during stop()."""
        try:
            return self._loop is not None and asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False


def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
    """The client thread: run the loop until stop(), then close it and the threads its HTTP calls use."""
    executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="wbskt-http")
    loop.set_default_executor(executor)
    asyncio.set_event_loop(loop)
    try:
        loop.run_forever()
    finally:
        # Not waited for: an HTTP call in flight finishes (or times out) on its own, and nothing reads it.
        executor.shutdown(wait=False)
        loop.close()


def _log_failure(task: "asyncio.Future[Any]") -> None:
    if not task.cancelled() and task.exception() is not None:
        log.error("wbskt: a call made from a handler failed", exc_info=task.exception())


async def _call(handler: Optional[Handler], *args: Any) -> None:
    if handler is None:
        return
    result = handler(*args)
    if inspect.isawaitable(result):
        await result


async def _post(url: str, body: Dict[str, Any]):
    return await asyncio.get_running_loop().run_in_executor(None, _http.post_json, url, body)


def _refusal(step: str, status: int, body: Any) -> str:
    """``registration answered 404 POLICY_NOT_FOUND: Invalid registration PIN.``, from the Problem Details."""
    text = f"{step} answered {status}"
    if isinstance(body, dict):
        if body.get("code"):
            text += f" {body['code']}"
        detail = body.get("detail") or body.get("title")
        if detail:
            text += f": {detail}"
    return text


def _check_type(message_type: str) -> None:
    if not message_type or len(message_type) > protocol.MAX_TYPE_LENGTH:
        raise ValueError(f"type must be 1-{protocol.MAX_TYPE_LENGTH} characters")
    if protocol.is_reserved(message_type):
        raise ValueError(f"'{message_type}' is reserved for the platform")


def _message_frame(message_type: str, payload: Any) -> str:
    _check_type(message_type)
    return _checked_size(_encode(message_type, payload, sent_at=_now()))


def _state_frame(values: Dict[str, Any]) -> str:
    return _checked_size(_encode(protocol.STATE_REPORT, dict(values), sent_at=_now()))


def _checked_size(frame: str) -> str:
    if len(frame.encode("utf-8")) > protocol.MAX_MESSAGE_BYTES:
        raise ValueError(f"message is over {protocol.MAX_MESSAGE_BYTES} bytes")
    return frame


def _encode(message_type: str, payload: Any, sent_at: Optional[str] = None) -> str:
    message: Dict[str, Any] = {"type": message_type, "payload": payload}
    if sent_at is not None:
        message["sentAt"] = sent_at
    try:
        # allow_nan=False: NaN and Infinity aren't JSON, and the gateway drops the whole message for them.
        # ensure_ascii=False: the 64 KB limit counts UTF-8 bytes, not \u escapes.
        return json.dumps(message, separators=(",", ":"), default=_json_default, allow_nan=False, ensure_ascii=False)
    except (TypeError, ValueError) as error:
        raise ValueError(f"payload can't be sent as JSON: {error}") from error


def _json_default(value: Any) -> Any:
    """Numbers stay numbers (numpy scalars, Decimal), so readings are charted rather than stored as text."""
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, (numbers.Real, decimal.Decimal)):
        return float(value)
    item = getattr(value, "item", None)
    if callable(item):
        # A numpy scalar that isn't registered as a number, such as numpy.bool_.
        return item()
    if isinstance(value, (datetime, date, dt_time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_time(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    # .NET writes seven fractional digits; fromisoformat before 3.11 takes at most six.
    match = _ISO_TIME.match(value.strip())
    if match is None:
        return None
    try:
        parsed = datetime.fromisoformat(match["base"] + (match["fraction"] or "")[:7] + (match["zone"] or "").upper().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _retry_after(headers) -> Optional[float]:
    """The Retry-After seconds, or None when the header is missing or not a number of seconds."""
    for key, value in headers.items():
        if key.lower() == "retry-after":
            try:
                seconds = float(value)
            except ValueError:
                return None
            # "inf" and "nan" parse as floats but are no wait to honour.
            return max(0.0, seconds) if math.isfinite(seconds) else None
    return None
