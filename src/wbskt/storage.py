"""Where a client keeps the credentials it got when it registered."""

import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Optional, Protocol, Tuple

Credentials = Tuple[Optional[str], Optional[str]]


class Storage(Protocol):
    def load(self) -> Credentials: ...

    def save(self, client_ref_id: str, secret: str) -> None: ...

    def clear(self) -> None: ...


class FileStorage:
    """Credentials in a JSON file, so the client registers once and signs in with them afterwards.

    The file is ``{"clientRefId": ..., "secret": ...}``, the same file the .NET SDK writes at the same
    path, so a client moved from one SDK to the other keeps its credentials. A file an older .NET SDK
    wrote (``RefId``, ``Secret``) is read too.
    """

    def __init__(self, path) -> None:
        self.path = Path(path)

    @classmethod
    def for_client(cls, client_name: str) -> "FileStorage":
        """``wbskt/<name>.json`` in the user's data folder (``~/.local/share`` on Linux).

        ``\\ / : * ? " < > |`` and control characters become ``_``, as in the .NET SDK, so names that
        differ only in those share a file.
        """
        safe = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", client_name)
        return cls(_data_dir() / "wbskt" / f"{safe}.json")

    def load(self) -> Credentials:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8-sig"))
            ref_id = data["clientRefId"] if "clientRefId" in data else data["RefId"]
            secret = data["secret"] if "secret" in data else data["Secret"]
        except (OSError, ValueError, KeyError, TypeError):
            return None, None
        if not isinstance(ref_id, str) or not isinstance(secret, str) or not ref_id or not secret:
            return None, None
        return ref_id, secret

    def save(self, client_ref_id: str, secret: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Written beside it and moved over it, so a crash never leaves half a file. Created readable by
        # this user only, and named per save so two processes saving at once don't collide.
        tmp = self.path.with_name(f"{self.path.name}.{uuid.uuid4().hex}.tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(json.dumps({"clientRefId": client_ref_id, "secret": secret}, separators=(",", ":")).encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        finally:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass

    def clear(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


class MemoryStorage:
    """Credentials held in memory only: for tests, or a client that is given them some other way."""

    def __init__(self, client_ref_id: Optional[str] = None, secret: Optional[str] = None) -> None:
        self._creds: Credentials = (client_ref_id, secret)

    def load(self) -> Credentials:
        return self._creds

    def save(self, client_ref_id: str, secret: str) -> None:
        self._creds = (client_ref_id, secret)

    def clear(self) -> None:
        self._creds = (None, None)


def _data_dir() -> Path:
    if sys.platform == "win32":
        return Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support"
    return Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local" / "share")
