"""Integration tests that run against a real HDFS cluster.

Warning: these tests will destroy everything under the test directory
(``/webhdfspy_test`` by default).

Requires a running Hadoop cluster with WebHDFS and append enabled.
Run with: ``pytest tests/test_integration.py --integration``

The cluster is configured through the environment:

===========================  =========================  ====================
Variable                     Meaning                    Default
===========================  =========================  ====================
``WEBHDFSPY_TEST_HOST``      namenode host              ``localhost``
``WEBHDFSPY_TEST_PORT``      namenode HTTP port         ``9870``
``WEBHDFSPY_TEST_USER``      HDFS username              current OS user
``WEBHDFSPY_TEST_DIR``       scratch directory          ``/webhdfspy_test``
``WEBHDFSPY_TEST_SCHEME``    ``http`` or ``https``      ``http``
``WEBHDFSPY_TEST_CA``        CA bundle for ``https``    system trust store
===========================  =========================  ====================
"""
import getpass
import os

import pytest

import webhdfspy

pytestmark = pytest.mark.integration

HOST = os.environ.get("WEBHDFSPY_TEST_HOST", "localhost")
PORT = int(os.environ.get("WEBHDFSPY_TEST_PORT", "9870"))
USERNAME = os.environ.get("WEBHDFSPY_TEST_USER") or getpass.getuser()
TEST_DIR_PATH = os.environ.get("WEBHDFSPY_TEST_DIR", "/webhdfspy_test")
SCHEME = os.environ.get("WEBHDFSPY_TEST_SCHEME", "http")
VERIFY = os.environ.get("WEBHDFSPY_TEST_CA") or True


def make_client(host=None, **kwargs):
    """Build a client pointed at the cluster under test."""
    kwargs.setdefault("scheme", SCHEME)
    kwargs.setdefault("verify", VERIFY)
    return webhdfspy.WebHDFSClient(host or HOST, PORT, USERNAME, **kwargs)


@pytest.fixture()
def client():
    with make_client() as c:
        c.mkdir(TEST_DIR_PATH)
        try:
            yield c
        finally:
            c.remove(TEST_DIR_PATH, True)


class TestDirOperations:
    def test_mkdir(self, client):
        client.mkdir(TEST_DIR_PATH + "/subdir")
        dir_content = client.listdir(TEST_DIR_PATH)
        dir_filenames = [d["pathSuffix"] for d in dir_content]
        assert "subdir" in dir_filenames

        client.remove(TEST_DIR_PATH + "/subdir")
        dir_content = client.listdir(TEST_DIR_PATH)
        dir_filenames = [d["pathSuffix"] for d in dir_content]
        assert "subdir" not in dir_filenames

    def test_listdir_empty(self, client):
        assert client.listdir(TEST_DIR_PATH) == []

    def test_listdir_missing_raises(self, client):
        with pytest.raises(webhdfspy.WebHDFSRemoteException) as exc_info:
            client.listdir(TEST_DIR_PATH + "/does_not_exist")
        assert exc_info.value.status_code == 404
        assert "FileNotFoundException" in exc_info.value.exception


class TestWriteOperations:
    def test_create(self, client):
        client.create(TEST_DIR_PATH + "/foo.txt", "foobar")
        dir_content = client.listdir(TEST_DIR_PATH)
        dir_filenames = [d["pathSuffix"] for d in dir_content]
        assert "foo.txt" in dir_filenames

    def test_overwrite(self, client):
        client.create(TEST_DIR_PATH + "/foobar.txt", "foobar")
        client.create(TEST_DIR_PATH + "/foobar.txt", "barfoo", overwrite=True)
        file_data = client.open(TEST_DIR_PATH + "/foobar.txt")
        assert file_data == "barfoo"

    def test_no_overwrite_raises(self, client):
        client.create(TEST_DIR_PATH + "/once.txt", "first")
        with pytest.raises(webhdfspy.WebHDFSRemoteException):
            client.create(TEST_DIR_PATH + "/once.txt", "second", overwrite=False)

    def test_append(self, client):
        client.create(TEST_DIR_PATH + "/barfoo.txt", "foo", overwrite=True)
        client.append(TEST_DIR_PATH + "/barfoo.txt", "bar")
        file_data = client.open(TEST_DIR_PATH + "/barfoo.txt")
        assert file_data == "foobar"

    def test_copyfromlocal(self, client, tmp_path):
        src = tmp_path / "upload.bin"
        blob = bytes(range(256)) * 40
        src.write_bytes(blob)
        client.copyfromlocal(str(src), TEST_DIR_PATH + "/upload.bin")
        assert client.read(TEST_DIR_PATH + "/upload.bin") == blob

    def test_copyfromlocal_missing_file(self, client, tmp_path):
        with pytest.raises(webhdfspy.WebHDFSException, match="doesn't exist"):
            client.copyfromlocal(str(tmp_path / "nope"), TEST_DIR_PATH + "/nope")


class TestNonAsciiPayloads:
    """Multi-byte text must survive a write/read round trip intact.

    ``requests`` sizes a ``str`` body by character count, so without explicit
    encoding the DataNode truncates the trailing bytes of any such payload.
    """

    def test_create_multibyte_text(self, client):
        content = "café ñandú 日本語 — ok"
        client.create(TEST_DIR_PATH + "/utf8.txt", content, overwrite=True)
        assert client.open(TEST_DIR_PATH + "/utf8.txt") == content

    def test_create_multibyte_length_on_disk(self, client):
        content = "café"
        client.create(TEST_DIR_PATH + "/len.txt", content, overwrite=True)
        status = client.status(TEST_DIR_PATH + "/len.txt")
        assert status["length"] == len(content.encode())

    def test_append_multibyte_text(self, client):
        client.create(TEST_DIR_PATH + "/app.txt", "héllo ", overwrite=True)
        client.append(TEST_DIR_PATH + "/app.txt", "wörld")
        assert client.open(TEST_DIR_PATH + "/app.txt") == "héllo wörld"

    def test_custom_encoding_round_trip(self, client):
        content = "café"
        client.create(
            TEST_DIR_PATH + "/latin.txt", content, overwrite=True, encoding="latin-1"
        )
        assert client.open(TEST_DIR_PATH + "/latin.txt", encoding="latin-1") == content


class TestSpecialCharacterPaths:
    """Filenames HDFS allows but that break an unencoded URL."""

    NAMES = [
        "report?draft.txt",
        "report#1.txt",
        "a b.txt",
        "100%.txt",
        "café.txt",
        "plus+and&amp.txt",
        "semi;colon.txt",
        "equals=sign.txt",
    ]

    @pytest.mark.parametrize("name", NAMES)
    def test_round_trip(self, client, name):
        path = f"{TEST_DIR_PATH}/{name}"
        client.create(path, f"contents of {name}", overwrite=True)
        assert client.open(path) == f"contents of {name}"

    def test_exact_names_listed(self, client):
        for name in self.NAMES:
            client.create(f"{TEST_DIR_PATH}/{name}", "x", overwrite=True)
        listed = sorted(d["pathSuffix"] for d in client.listdir(TEST_DIR_PATH))
        assert listed == sorted(self.NAMES)

    def test_remove_targets_exact_file(self, client):
        """A '#' must not truncate the path and delete the wrong thing."""
        for name in ("report#1.txt", "report.txt"):
            client.create(f"{TEST_DIR_PATH}/{name}", "x", overwrite=True)

        client.remove(f"{TEST_DIR_PATH}/report#1.txt")

        remaining = sorted(d["pathSuffix"] for d in client.listdir(TEST_DIR_PATH))
        assert remaining == ["report.txt"]

    def test_status_of_special_name(self, client):
        path = f"{TEST_DIR_PATH}/query?a=b.txt"
        client.create(path, "12345", overwrite=True)
        assert client.status(path)["length"] == 5

    def test_rename_to_special_name(self, client):
        client.create(TEST_DIR_PATH + "/plain.txt", "data", overwrite=True)
        client.rename(TEST_DIR_PATH + "/plain.txt", TEST_DIR_PATH + "/re named#2.txt")
        listed = [d["pathSuffix"] for d in client.listdir(TEST_DIR_PATH)]
        assert listed == ["re named#2.txt"]


class TestReadOperations:
    def test_read_returns_bytes(self, client):
        blob = bytes(range(256))
        client.create(TEST_DIR_PATH + "/blob.bin", blob, overwrite=True)
        assert client.read(TEST_DIR_PATH + "/blob.bin") == blob

    def test_binary_survives_round_trip(self, client):
        """Non-UTF-8 bytes must not be mangled by text decoding."""
        blob = bytes([0xFF, 0xFE, 0x00, 0x80, 0x41]) * 100
        client.create(TEST_DIR_PATH + "/raw.bin", blob, overwrite=True)
        assert client.read(TEST_DIR_PATH + "/raw.bin") == blob

    def test_offset_and_length(self, client):
        client.create(TEST_DIR_PATH + "/abc.txt", "abcdefghij", overwrite=True)
        assert client.read(TEST_DIR_PATH + "/abc.txt", offset=2, length=3) == b"cde"

    def test_stream(self, client):
        blob = bytes(range(256)) * 500  # 128 KiB, several chunks
        client.create(TEST_DIR_PATH + "/big.bin", blob, overwrite=True)
        with client.stream(TEST_DIR_PATH + "/big.bin", chunk_size=4096) as chunks:
            assert b"".join(chunks) == blob

    def test_stream_missing_file_raises(self, client):
        with pytest.raises(webhdfspy.WebHDFSRemoteException):
            with client.stream(TEST_DIR_PATH + "/absent.bin"):
                pass

    def test_copytolocal(self, client, tmp_path):
        blob = bytes(range(256)) * 500
        client.create(TEST_DIR_PATH + "/down.bin", blob, overwrite=True)
        dest = tmp_path / "down.bin"
        assert client.copytolocal(TEST_DIR_PATH + "/down.bin", str(dest)) is True
        assert dest.read_bytes() == blob

    def test_round_trip_local_file(self, client, tmp_path):
        blob = os.urandom(200_000)
        src, dest = tmp_path / "src.bin", tmp_path / "dest.bin"
        src.write_bytes(blob)
        client.copyfromlocal(str(src), TEST_DIR_PATH + "/rt.bin")
        client.copytolocal(TEST_DIR_PATH + "/rt.bin", str(dest))
        assert dest.read_bytes() == blob


class TestPermissionOperations:
    def test_chmod(self, client):
        client.mkdir(TEST_DIR_PATH + "/chmodtest", "777")
        dir_content = client.listdir(TEST_DIR_PATH)
        created_dir = [d for d in dir_content if d["pathSuffix"] == "chmodtest"]
        assert created_dir[0]["permission"] == "777"

        client.chmod(TEST_DIR_PATH + "/chmodtest", "444")
        dir_content = client.listdir(TEST_DIR_PATH)
        created_dir = [d for d in dir_content if d["pathSuffix"] == "chmodtest"]
        assert created_dir[0]["permission"] == "444"

    def test_set_owner_group(self, client):
        client.create(TEST_DIR_PATH + "/owned.txt", "x", overwrite=True)
        client.set_owner(TEST_DIR_PATH + "/owned.txt", group="supergroup")
        assert client.status(TEST_DIR_PATH + "/owned.txt")["group"] == "supergroup"

    def test_set_owner_requires_an_argument(self, client):
        with pytest.raises(webhdfspy.WebHDFSException, match="At least one"):
            client.set_owner(TEST_DIR_PATH)

    def test_set_times(self, client):
        client.create(TEST_DIR_PATH + "/timed.txt", "x", overwrite=True)
        client.set_times(TEST_DIR_PATH + "/timed.txt", modificationtime=1000000000)
        status = client.status(TEST_DIR_PATH + "/timed.txt")
        assert status["modificationTime"] == 1000000000


class TestRenameOperations:
    def test_rename_file(self, client):
        client.create(TEST_DIR_PATH + "/foo.txt", "foobar")
        client.rename(TEST_DIR_PATH + "/foo.txt", TEST_DIR_PATH + "/bar.txt")
        dir_content = client.listdir(TEST_DIR_PATH)
        dir_filenames = [d["pathSuffix"] for d in dir_content]
        assert "bar.txt" in dir_filenames

    def test_rename_dir(self, client):
        client.mkdir(TEST_DIR_PATH + "/foo")
        client.rename(TEST_DIR_PATH + "/foo", TEST_DIR_PATH + "/bar")
        dir_content = client.listdir(TEST_DIR_PATH)
        dir_filenames = [d["pathSuffix"] for d in dir_content]
        assert "bar" in dir_filenames


class TestReplicationOperations:
    def test_replication(self, client):
        client.create(TEST_DIR_PATH + "/foo.txt", "foobar", True)
        client.set_replication(TEST_DIR_PATH + "/foo.txt", 2)
        file_status = client.status(TEST_DIR_PATH + "/foo.txt")
        assert file_status["replication"] == 2

    def test_negative_replication(self, client):
        client.create(TEST_DIR_PATH + "/foo.txt", "foobar", True)
        with pytest.raises(webhdfspy.WebHDFSRemoteException):
            client.set_replication(TEST_DIR_PATH + "/foo.txt", -3)


class TestChecksumOperations:
    def test_checksum(self, client):
        client.create(TEST_DIR_PATH + "/foo.txt", "foobar")
        checksum = client.get_checksum(TEST_DIR_PATH + "/foo.txt")
        assert checksum["algorithm"]
        assert checksum["length"] == 28

    def test_checksum_is_content_dependent(self, client):
        client.create(TEST_DIR_PATH + "/a.txt", "foobar", overwrite=True)
        client.create(TEST_DIR_PATH + "/b.txt", "foobar", overwrite=True)
        client.create(TEST_DIR_PATH + "/c.txt", "different", overwrite=True)

        same_a = client.get_checksum(TEST_DIR_PATH + "/a.txt")["bytes"]
        same_b = client.get_checksum(TEST_DIR_PATH + "/b.txt")["bytes"]
        other = client.get_checksum(TEST_DIR_PATH + "/c.txt")["bytes"]
        assert same_a == same_b
        assert same_a != other


class TestContentSummary:
    def test_content_summary(self, client):
        client.mkdir(TEST_DIR_PATH + "/summary")
        client.create(TEST_DIR_PATH + "/summary/a.txt", "12345", overwrite=True)
        summary = client.get_content_summary(TEST_DIR_PATH + "/summary")
        assert summary["fileCount"] == 1
        assert summary["length"] == 5


class TestEnvironHome:
    def test_environ_home(self, client):
        assert client.environ_home() == f"/user/{USERNAME}"


class TestFailover:
    """A dead namenode in the list must not stop the client from working."""

    def test_falls_over_to_reachable_namenode(self):
        """The first namenode is unreachable, so the second serves the request."""
        with make_client(["nonexistent.invalid", HOST], timeout=10.0) as c:
            assert c.environ_home() == f"/user/{USERNAME}"
            assert c.namenode_url.startswith(f"{SCHEME}://{HOST}:")

    def test_all_namenodes_unreachable(self):
        with make_client(
            ["nonexistent.invalid", "also-nonexistent.invalid"], timeout=10.0
        ) as c, pytest.raises(webhdfspy.WebHDFSConnectionError, match="No active"):
            c.listdir("/")


@pytest.mark.skipif(SCHEME != "https", reason="cluster is not served over TLS")
class TestTLS:
    def test_ca_bundle_accepted(self, client):
        """The whole two-step create/read path works over TLS."""
        client.create(TEST_DIR_PATH + "/tls.txt", "over tls", overwrite=True)
        assert client.open(TEST_DIR_PATH + "/tls.txt") == "over tls"

    @pytest.mark.skipif(
        VERIFY is True, reason="cluster cert is already in the system trust store"
    )
    def test_untrusted_certificate_rejected(self):
        """Verification failures surface as our own exception, not a raw one."""
        with make_client(verify=True) as c, pytest.raises(
            webhdfspy.WebHDFSConnectionError
        ) as exc_info:
            c.listdir("/")
        assert "SSL" in type(exc_info.value.cause).__name__

    def test_verify_disabled_allows_self_signed(self):
        with make_client(verify=False) as c:
            assert c.environ_home() == f"/user/{USERNAME}"


class TestErrorHandling:
    def test_remote_exception_details(self, client):
        with pytest.raises(webhdfspy.WebHDFSRemoteException) as exc_info:
            client.status(TEST_DIR_PATH + "/missing.txt")
        error = exc_info.value
        assert error.status_code == 404
        assert "FileNotFoundException" in error.exception
        assert error.java_class_name == "java.io.FileNotFoundException"
        assert str(error)

    def test_connection_error_on_wrong_port(self):
        with webhdfspy.WebHDFSClient(HOST, 1, USERNAME, timeout=5.0) as c:
            with pytest.raises(webhdfspy.WebHDFSConnectionError):
                c.listdir("/")
