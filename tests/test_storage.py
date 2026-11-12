import os
import stat
import sys

import pytest

from wbskt import FileStorage


def test_round_trips_and_clears(tmp_path):
    storage = FileStorage(tmp_path / "creds.json")
    assert storage.load() == (None, None)

    storage.save("ref", "secret")
    assert FileStorage(tmp_path / "creds.json").load() == ("ref", "secret")

    storage.clear()
    storage.clear()
    assert storage.load() == (None, None)


def test_only_the_owner_can_read_it(tmp_path):
    storage = FileStorage(tmp_path / "creds.json")
    storage.save("ref", "secret")

    if sys.platform != "win32":
        assert os.stat(storage.path).st_mode & 0o077 == 0


def test_a_corrupt_file_reads_as_no_credentials(tmp_path):
    (tmp_path / "creds.json").write_text("{not json")

    assert FileStorage(tmp_path / "creds.json").load() == (None, None)


def test_client_names_become_safe_file_names(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    path = FileStorage.for_client('pi/kitchen:"1"').path

    assert path.name == "pi_kitchen__1_.json"


def test_a_file_the_dotnet_sdk_wrote_is_read(tmp_path):
    current = tmp_path / "current.json"
    older = tmp_path / "older.json"
    current.write_text('{"clientRefId":"5c0d6c8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","secret":"from-dotnet"}', encoding="utf-8")
    older.write_text('{"RefId":"5c0d6c8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","Secret":"from-0.9.0-beta.1"}', encoding="utf-8")

    assert FileStorage(current).load() == ("5c0d6c8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e", "from-dotnet")
    assert FileStorage(older).load() == ("5c0d6c8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e", "from-0.9.0-beta.1")


def test_a_file_saved_with_a_byte_order_mark_is_read(tmp_path):
    path = tmp_path / "c.json"
    path.write_text('{"clientRefId": "abc", "secret": "s"}', encoding="utf-8-sig")
    assert FileStorage(path).load() == ("abc", "s")


def test_a_null_value_reads_as_no_credentials(tmp_path):
    path = tmp_path / "c.json"
    path.write_text('{"clientRefId": null, "secret": "s"}', encoding="utf-8")
    assert FileStorage(path).load() == (None, None)


def test_a_temporary_file_left_by_an_interrupted_save_does_not_block_the_next(tmp_path):
    path = tmp_path / "c.json"
    (tmp_path / "c.json.tmp").write_text("partial", encoding="utf-8")
    storage = FileStorage(path)
    storage.save("abc", "s")
    assert storage.load() == ("abc", "s")
    assert not (tmp_path / "c.json.tmp").exists()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX modes")
def test_the_file_is_readable_by_this_user_only(tmp_path):
    path = tmp_path / "c.json"
    FileStorage(path).save("abc", "s")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
