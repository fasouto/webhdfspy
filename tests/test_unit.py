"""Unit tests for webhdfspy using the ``responses`` library to mock HTTP."""

import pytest
import requests
import responses
from responses import matchers

from webhdfspy import (
    WebHDFSClient,
    WebHDFSConnectionError,
    WebHDFSException,
    WebHDFSRemoteException,
)

BASE = "http://localhost:50070/webhdfs/v1"
DATANODE = "http://datanode:50075/webhdfs/v1"


@pytest.fixture()
def client():
    with WebHDFSClient("localhost", 50070, username="testuser") as c:
        yield c


@pytest.fixture()
def anon_client():
    """Client with no username."""
    with WebHDFSClient("localhost", 50070) as c:
        yield c


# ------------------------------------------------------------------
# Init / connection
# ------------------------------------------------------------------

class TestInit:
    def test_default_scheme(self):
        c = WebHDFSClient("host", 9870)
        assert c.namenode_url.startswith("http://")
        c.close()

    def test_https_scheme(self):
        c = WebHDFSClient("host", 9870, scheme="https")
        assert c.namenode_url.startswith("https://")
        c.close()

    def test_timeout_stored(self):
        c = WebHDFSClient("host", 9870, timeout=30.0)
        assert c.timeout == 30.0
        c.close()

    def test_context_manager(self):
        with WebHDFSClient("host", 9870) as c:
            assert c._session is not None


# ------------------------------------------------------------------
# user.name parameter handling
# ------------------------------------------------------------------

class TestUserNameParam:
    @responses.activate
    def test_username_included(self, client):
        responses.add(
            responses.GET,
            f"{BASE}/",
            json={"FileStatuses": {"FileStatus": []}},
            status=200,
        )
        client.listdir("/")
        assert "user.name=testuser" in responses.calls[0].request.url

    @responses.activate
    def test_username_absent_when_none(self, anon_client):
        responses.add(
            responses.GET,
            f"{BASE}/",
            json={"FileStatuses": {"FileStatus": []}},
            status=200,
        )
        anon_client.listdir("/")
        assert "user.name" not in responses.calls[0].request.url


# ------------------------------------------------------------------
# Listdir
# ------------------------------------------------------------------

class TestListdir:
    @responses.activate
    def test_success(self, client):
        payload = {
            "FileStatuses": {
                "FileStatus": [
                    {"pathSuffix": "foo", "type": "DIRECTORY"},
                    {"pathSuffix": "bar.txt", "type": "FILE"},
                ]
            }
        }
        responses.add(responses.GET, f"{BASE}/", json=payload, status=200)
        result = client.listdir("/")
        assert len(result) == 2
        assert result[0]["pathSuffix"] == "foo"

    @responses.activate
    def test_not_found(self, client):
        payload = {
            "RemoteException": {
                "exception": "FileNotFoundException",
                "javaClassName": "java.io.FileNotFoundException",
                "message": "/nonexistent does not exist",
            }
        }
        responses.add(responses.GET, f"{BASE}/nonexistent", json=payload, status=404)
        with pytest.raises(WebHDFSRemoteException) as exc_info:
            client.listdir("/nonexistent")
        assert exc_info.value.status_code == 404
        assert exc_info.value.exception == "FileNotFoundException"


# ------------------------------------------------------------------
# Mkdir
# ------------------------------------------------------------------

class TestMkdir:
    @responses.activate
    def test_success(self, client):
        responses.add(
            responses.PUT, f"{BASE}/testdir", json={"boolean": True}, status=200,
        )
        assert client.mkdir("/testdir") is True

    @responses.activate
    def test_with_permission(self, client):
        responses.add(
            responses.PUT, f"{BASE}/testdir", json={"boolean": True}, status=200,
        )
        client.mkdir("/testdir", permission="755")
        assert "permission=755" in responses.calls[0].request.url

    @responses.activate
    def test_no_permission_param_when_none(self, client):
        responses.add(
            responses.PUT, f"{BASE}/testdir", json={"boolean": True}, status=200,
        )
        client.mkdir("/testdir")
        assert "permission" not in responses.calls[0].request.url


# ------------------------------------------------------------------
# Remove
# ------------------------------------------------------------------

class TestRemove:
    @responses.activate
    def test_success(self, client):
        responses.add(
            responses.DELETE, f"{BASE}/testdir", json={"boolean": True}, status=200,
        )
        assert client.remove("/testdir") is True

    @responses.activate
    def test_recursive(self, client):
        responses.add(
            responses.DELETE, f"{BASE}/testdir", json={"boolean": True}, status=200,
        )
        client.remove("/testdir", recursive=True)
        assert "recursive=true" in responses.calls[0].request.url


# ------------------------------------------------------------------
# Rename
# ------------------------------------------------------------------

class TestRename:
    @responses.activate
    def test_success(self, client):
        responses.add(
            responses.PUT, f"{BASE}/src", json={"boolean": True}, status=200,
        )
        assert client.rename("/src", "/dst") is True
        assert "destination=%2Fdst" in responses.calls[0].request.url


# ------------------------------------------------------------------
# Open
# ------------------------------------------------------------------

class TestOpen:
    @responses.activate
    def test_success(self, client):
        responses.add(
            responses.GET, f"{BASE}/file.txt", body="hello world", status=200,
        )
        result = client.open("/file.txt")
        assert result == "hello world"

    @responses.activate
    def test_error_raises(self, client):
        payload = {
            "RemoteException": {
                "exception": "FileNotFoundException",
                "javaClassName": "java.io.FileNotFoundException",
                "message": "/missing does not exist",
            }
        }
        responses.add(
            responses.GET, f"{BASE}/missing", json=payload, status=404,
        )
        with pytest.raises(WebHDFSRemoteException):
            client.open("/missing")


# ------------------------------------------------------------------
# Status
# ------------------------------------------------------------------

class TestStatus:
    @responses.activate
    def test_success(self, client):
        payload = {
            "FileStatus": {
                "pathSuffix": "",
                "type": "FILE",
                "length": 123,
            }
        }
        responses.add(responses.GET, f"{BASE}/file.txt", json=payload, status=200)
        result = client.status("/file.txt")
        assert result["type"] == "FILE"
        assert result["length"] == 123


# ------------------------------------------------------------------
# Chmod
# ------------------------------------------------------------------

class TestChmod:
    @responses.activate
    def test_success(self, client):
        responses.add(responses.PUT, f"{BASE}/file.txt", body="", status=200)
        assert client.chmod("/file.txt", "755") is True


# ------------------------------------------------------------------
# Create (two-step redirect)
# ------------------------------------------------------------------

class TestCreate:
    @responses.activate
    def test_success(self, client):
        # Step 1: NameNode returns 307 with Location header
        responses.add(
            responses.PUT,
            f"{BASE}/newfile.txt",
            status=307,
            headers={"Location": f"{DATANODE}/newfile.txt"},
        )
        # Step 2: DataNode returns 201
        responses.add(responses.PUT, f"{DATANODE}/newfile.txt", status=201)

        assert client.create("/newfile.txt", "data") is True

    @responses.activate
    def test_missing_redirect(self, client):
        responses.add(responses.PUT, f"{BASE}/newfile.txt", status=307)
        with pytest.raises(WebHDFSException, match="did not return a redirect"):
            client.create("/newfile.txt", "data")

    @responses.activate
    def test_overwrite_param(self, client):
        responses.add(
            responses.PUT,
            f"{BASE}/newfile.txt",
            status=307,
            headers={"Location": f"{DATANODE}/newfile.txt"},
        )
        responses.add(responses.PUT, f"{DATANODE}/newfile.txt", status=201)
        client.create("/newfile.txt", "data", overwrite=True)
        assert "overwrite=true" in responses.calls[0].request.url


# ------------------------------------------------------------------
# Append (two-step redirect)
# ------------------------------------------------------------------

class TestAppend:
    @responses.activate
    def test_success(self, client):
        responses.add(
            responses.POST,
            f"{BASE}/file.txt",
            status=307,
            headers={"Location": f"{DATANODE}/file.txt"},
        )
        responses.add(responses.POST, f"{DATANODE}/file.txt", status=200)

        assert client.append("/file.txt", "more data") is True

    @responses.activate
    def test_no_op_param_on_datanode(self, client):
        """Bug #9: op=APPEND should NOT be sent to DataNode."""
        responses.add(
            responses.POST,
            f"{BASE}/file.txt",
            status=307,
            headers={"Location": f"{DATANODE}/file.txt"},
        )
        responses.add(responses.POST, f"{DATANODE}/file.txt", status=200)

        client.append("/file.txt", "data")
        datanode_request = responses.calls[1].request
        assert "op=APPEND" not in (datanode_request.url or "")

    @responses.activate
    def test_missing_redirect(self, client):
        responses.add(responses.POST, f"{BASE}/file.txt", status=307)
        with pytest.raises(WebHDFSException, match="did not return a redirect"):
            client.append("/file.txt", "data")


# ------------------------------------------------------------------
# SetOwner
# ------------------------------------------------------------------

class TestSetOwner:
    @responses.activate
    def test_success(self, client):
        responses.add(responses.PUT, f"{BASE}/file.txt", body="", status=200)
        assert client.set_owner("/file.txt", owner="newowner") is True
        assert "owner=newowner" in responses.calls[0].request.url

    @responses.activate
    def test_group_only(self, client):
        responses.add(responses.PUT, f"{BASE}/file.txt", body="", status=200)
        assert client.set_owner("/file.txt", group="newgroup") is True
        assert "group=newgroup" in responses.calls[0].request.url

    def test_no_args_raises(self, client):
        with pytest.raises(WebHDFSException, match="At least one"):
            client.set_owner("/file.txt")


# ------------------------------------------------------------------
# SetTimes
# ------------------------------------------------------------------

class TestSetTimes:
    @responses.activate
    def test_success(self, client):
        responses.add(responses.PUT, f"{BASE}/file.txt", body="", status=200)
        result = client.set_times("/file.txt", modificationtime=1000, accesstime=2000)
        assert result is True
        assert "modificationtime=1000" in responses.calls[0].request.url
        assert "accesstime=2000" in responses.calls[0].request.url


# ------------------------------------------------------------------
# GetContentSummary
# ------------------------------------------------------------------

class TestGetContentSummary:
    @responses.activate
    def test_success(self, client):
        payload = {
            "ContentSummary": {
                "directoryCount": 2,
                "fileCount": 1,
                "length": 24930,
                "quota": -1,
                "spaceConsumed": 24930,
                "spaceQuota": -1,
            }
        }
        responses.add(responses.GET, f"{BASE}/dir", json=payload, status=200)
        result = client.get_content_summary("/dir")
        assert result["directoryCount"] == 2
        assert result["fileCount"] == 1


# ------------------------------------------------------------------
# GetChecksum (two-step redirect)
# ------------------------------------------------------------------

class TestGetChecksum:
    @responses.activate
    def test_success(self, client):
        responses.add(
            responses.GET,
            f"{BASE}/file.txt",
            status=307,
            headers={"Location": f"{DATANODE}/file.txt"},
        )
        checksum_payload = {
            "FileChecksum": {
                "algorithm": "MD5-of-1MD5-of-512CRC32C",
                "bytes": "abc123",
                "length": 28,
            }
        }
        responses.add(
            responses.GET, f"{DATANODE}/file.txt", json=checksum_payload, status=200,
        )
        result = client.get_checksum("/file.txt")
        assert result["algorithm"] == "MD5-of-1MD5-of-512CRC32C"

    @responses.activate
    def test_missing_redirect(self, client):
        responses.add(responses.GET, f"{BASE}/file.txt", status=307)
        with pytest.raises(WebHDFSException, match="did not return a redirect"):
            client.get_checksum("/file.txt")


# ------------------------------------------------------------------
# SetReplication
# ------------------------------------------------------------------

class TestSetReplication:
    @responses.activate
    def test_success(self, client):
        responses.add(
            responses.PUT, f"{BASE}/file.txt", json={"boolean": True}, status=200,
        )
        assert client.set_replication("/file.txt", 3) is True


# ------------------------------------------------------------------
# Delegation tokens
# ------------------------------------------------------------------

class TestDelegationTokens:
    @responses.activate
    def test_get(self, client):
        payload = {"Token": {"urlString": "JQAIaG9y..."}}
        responses.add(responses.GET, f"{BASE}/", json=payload, status=200)
        result = client.get_delegation_token("renewer")
        assert result["urlString"] == "JQAIaG9y..."

    @responses.activate
    def test_renew(self, client):
        responses.add(
            responses.PUT, f"{BASE}/", json={"long": 1609459200000}, status=200,
        )
        result = client.renew_delegation_token("sometoken")
        assert result == 1609459200000

    @responses.activate
    def test_cancel(self, client):
        responses.add(responses.PUT, f"{BASE}/", body="", status=200)
        assert client.cancel_delegation_token("sometoken") is True


# ------------------------------------------------------------------
# Error handling
# ------------------------------------------------------------------

class TestErrorHandling:
    @responses.activate
    def test_remote_exception_parsed(self, client):
        payload = {
            "RemoteException": {
                "exception": "AccessControlException",
                "javaClassName": "org.apache.hadoop.security.AccessControlException",
                "message": "Permission denied",
            }
        }
        responses.add(responses.GET, f"{BASE}/secret", json=payload, status=403)
        with pytest.raises(WebHDFSRemoteException) as exc_info:
            client.listdir("/secret")
        assert exc_info.value.status_code == 403
        assert exc_info.value.exception == "AccessControlException"
        assert "Permission denied" in str(exc_info.value)

    @responses.activate
    def test_generic_error(self, client):
        responses.add(
            responses.GET, f"{BASE}/bad", body="Internal Server Error", status=500,
        )
        with pytest.raises(WebHDFSException, match="status 500"):
            client.listdir("/bad")

    @responses.activate
    def test_connection_error(self):
        with WebHDFSClient("nonexistent.invalid", 50070) as c:
            with pytest.raises(WebHDFSConnectionError):
                c.listdir("/")


# ------------------------------------------------------------------
# EnvironHome
# ------------------------------------------------------------------

class TestEnvironHome:
    @responses.activate
    def test_success(self, client):
        responses.add(
            responses.GET, f"{BASE}/", json={"Path": "/user/testuser"}, status=200,
        )
        assert client.environ_home() == "/user/testuser"


# ------------------------------------------------------------------
# Path encoding
# ------------------------------------------------------------------

class TestPathEncoding:
    @pytest.mark.parametrize(
        ("path", "encoded"),
        [
            ("/a b/c.txt", "/a%20b/c.txt"),
            ("/report?draft.txt", "/report%3Fdraft.txt"),
            ("/report#1.txt", "/report%231.txt"),
            ("/100%25.txt", "/100%2525.txt"),
            ("/café/ñ.txt", "/caf%C3%A9/%C3%B1.txt"),
            ("/plain/file.txt", "/plain/file.txt"),
        ],
    )
    def test_encoding(self, path, encoded):
        assert WebHDFSClient._encode_path(path) == encoded

    def test_leading_slash_added(self):
        assert WebHDFSClient._encode_path("relative/path") == "/relative/path"

    @responses.activate
    def test_question_mark_addresses_right_file(self, client):
        """A '?' in a filename must not be parsed as the query separator."""
        responses.add(
            responses.DELETE,
            f"{BASE}/report%3Fdraft.txt",
            json={"boolean": True},
            status=200,
            match=[matchers.query_param_matcher(
                {"op": "DELETE", "recursive": "false", "user.name": "testuser"}
            )],
        )
        assert client.remove("/report?draft.txt") is True

    @responses.activate
    def test_hash_does_not_truncate_path(self, client):
        """A '#' in a filename must not be parsed as a fragment."""
        responses.add(
            responses.DELETE, f"{BASE}/data/report%231", json={"boolean": True},
            status=200,
        )
        client.remove("/data/report#1")
        assert responses.calls[0].request.url.startswith(
            f"{BASE}/data/report%231?"
        )


# ------------------------------------------------------------------
# Transport error wrapping
# ------------------------------------------------------------------

class TestTransportErrors:
    @responses.activate
    def test_read_timeout_wrapped(self, client):
        responses.add(
            responses.GET, f"{BASE}/", body=requests.exceptions.ReadTimeout("slow"),
        )
        with pytest.raises(WebHDFSConnectionError):
            client.listdir("/")

    @responses.activate
    def test_connect_timeout_wrapped(self, client):
        responses.add(
            responses.GET, f"{BASE}/", body=requests.exceptions.ConnectTimeout("nope"),
        )
        with pytest.raises(WebHDFSConnectionError):
            client.listdir("/")

    @responses.activate
    def test_datanode_timeout_wrapped(self, client):
        """Errors on the second (DataNode) hop are wrapped too."""
        responses.add(
            responses.PUT, f"{BASE}/f.txt", status=307,
            headers={"Location": f"{DATANODE}/f.txt"},
        )
        responses.add(
            responses.PUT, f"{DATANODE}/f.txt",
            body=requests.exceptions.ReadTimeout("slow"),
        )
        with pytest.raises(WebHDFSConnectionError):
            client.create("/f.txt", "data")

    def test_cause_preserved(self):
        with WebHDFSClient("nonexistent.invalid", 50070) as c:
            with pytest.raises(WebHDFSConnectionError) as exc_info:
                c.listdir("/")
        assert isinstance(exc_info.value.cause, requests.RequestException)


# ------------------------------------------------------------------
# Binary and streaming reads
# ------------------------------------------------------------------

class TestBinaryReads:
    @responses.activate
    def test_read_returns_bytes(self, client):
        blob = bytes(range(256))
        responses.add(responses.GET, f"{BASE}/blob.bin", body=blob, status=200)
        assert client.read("/blob.bin") == blob

    @responses.activate
    def test_open_decodes_utf8(self, client):
        responses.add(
            responses.GET, f"{BASE}/t.txt", body="café ñ".encode(),
            status=200,
        )
        assert client.open("/t.txt") == "café ñ"

    @responses.activate
    def test_open_custom_encoding(self, client):
        responses.add(
            responses.GET, f"{BASE}/t.txt", body="café".encode("latin-1"),
            status=200,
        )
        assert client.open("/t.txt", encoding="latin-1") == "café"

    @responses.activate
    def test_open_params(self, client):
        responses.add(responses.GET, f"{BASE}/t.txt", body=b"x", status=200)
        client.open("/t.txt", offset=10, length=5, buffersize=1024)
        url = responses.calls[0].request.url
        assert "offset=10" in url and "length=5" in url and "buffersize=1024" in url

    @responses.activate
    def test_stream_chunks(self, client):
        blob = b"0123456789" * 100
        responses.add(responses.GET, f"{BASE}/big.bin", body=blob, status=200)
        with client.stream("/big.bin", chunk_size=64) as chunks:
            assert b"".join(chunks) == blob

    @responses.activate
    def test_stream_raises_on_error(self, client):
        responses.add(
            responses.GET, f"{BASE}/missing",
            json={"RemoteException": {"exception": "FileNotFoundException",
                                      "message": "nope"}},
            status=404,
        )
        with pytest.raises(WebHDFSRemoteException), client.stream("/missing"):
            pass

    @responses.activate
    def test_copytolocal(self, client, tmp_path):
        blob = bytes(range(256)) * 20
        responses.add(responses.GET, f"{BASE}/blob.bin", body=blob, status=200)
        dest = tmp_path / "out.bin"
        assert client.copytolocal("/blob.bin", str(dest)) is True
        assert dest.read_bytes() == blob


# ------------------------------------------------------------------
# Auth / TLS / session configuration
# ------------------------------------------------------------------

class TestAuthConfig:
    def test_auth_applied_to_session(self):
        auth = ("user", "pass")
        with WebHDFSClient("h", 9870, auth=auth) as c:
            assert c._session.auth == auth

    def test_verify_ca_bundle(self):
        with WebHDFSClient("h", 9870, verify="/etc/ssl/ca.pem") as c:
            assert c._session.verify == "/etc/ssl/ca.pem"

    def test_verify_disabled(self):
        with WebHDFSClient("h", 9870, verify=False) as c:
            assert c._session.verify is False

    def test_client_cert(self):
        with WebHDFSClient("h", 9870, cert=("/c.pem", "/k.pem")) as c:
            assert c._session.cert == ("/c.pem", "/k.pem")

    def test_supplied_session_not_closed(self):
        session = requests.Session()
        with WebHDFSClient("h", 9870, session=session) as c:
            assert c._session is session
        # Exiting the context manager must not close a caller-owned session.
        assert session.adapters, "caller's session was closed"
        session.close()

    @responses.activate
    def test_supplied_session_used(self):
        session = requests.Session()
        session.headers["X-Custom"] = "yes"
        with WebHDFSClient("localhost", 50070, session=session) as c:
            responses.add(
                responses.GET, f"{BASE}/", json={"Path": "/user/x"}, status=200,
            )
            c.environ_home()
        assert responses.calls[0].request.headers["X-Custom"] == "yes"
        session.close()


# ------------------------------------------------------------------
# Delegation token authentication
# ------------------------------------------------------------------

class TestDelegationTokenAuth:
    @responses.activate
    def test_token_sent(self):
        with WebHDFSClient("localhost", 50070, token="TOKEN123") as c:
            responses.add(
                responses.GET, f"{BASE}/", json={"FileStatuses": {"FileStatus": []}},
                status=200,
            )
            c.listdir("/")
        assert "delegation=TOKEN123" in responses.calls[0].request.url

    @responses.activate
    def test_token_takes_precedence_over_username(self):
        with WebHDFSClient("localhost", 50070, "someuser", token="TOKEN123") as c:
            responses.add(
                responses.GET, f"{BASE}/", json={"FileStatuses": {"FileStatus": []}},
                status=200,
            )
            c.listdir("/")
        url = responses.calls[0].request.url
        assert "delegation=TOKEN123" in url
        assert "user.name" not in url

    @responses.activate
    def test_set_delegation_token_roundtrip(self, client):
        responses.add(
            responses.GET, f"{BASE}/", json={"Token": {"urlString": "ABC"}}, status=200,
        )
        token = client.get_delegation_token("renewer")
        client.set_delegation_token(token["urlString"])

        responses.add(
            responses.GET, f"{BASE}/dir", json={"FileStatuses": {"FileStatus": []}},
            status=200,
        )
        client.listdir("/dir")
        assert "delegation=ABC" in responses.calls[1].request.url

    @responses.activate
    def test_clearing_token_restores_username(self, client):
        client.set_delegation_token("ABC")
        client.set_delegation_token(None)
        responses.add(
            responses.GET, f"{BASE}/", json={"FileStatuses": {"FileStatus": []}},
            status=200,
        )
        client.listdir("/")
        assert "user.name=testuser" in responses.calls[0].request.url


# ------------------------------------------------------------------
# HA namenode failover
# ------------------------------------------------------------------

NN1 = "http://nn1:9870/webhdfs/v1"
NN2 = "http://nn2:9870/webhdfs/v1"

STANDBY_BODY = {
    "RemoteException": {
        "exception": "StandbyException",
        "javaClassName": "org.apache.hadoop.ipc.StandbyException",
        "message": "Operation category READ is not supported in state standby",
    }
}


@pytest.fixture()
def ha_client():
    with WebHDFSClient(["nn1", "nn2"], 9870, username="testuser") as c:
        yield c


class TestHAFailover:
    def test_single_host_still_works(self):
        with WebHDFSClient("h", 9870) as c:
            assert c.hosts == ["h"]
            assert c.namenode_url == "http://h:9870/webhdfs/v1"

    def test_host_list_accepted(self, ha_client):
        assert ha_client.hosts == ["nn1", "nn2"]

    def test_empty_host_list_rejected(self):
        with pytest.raises(WebHDFSException, match="At least one host"):
            WebHDFSClient([], 9870)

    @responses.activate
    def test_failover_on_standby(self, ha_client):
        responses.add(responses.GET, f"{NN1}/", json=STANDBY_BODY, status=403)
        responses.add(
            responses.GET, f"{NN2}/", json={"FileStatuses": {"FileStatus": []}},
            status=200,
        )
        assert ha_client.listdir("/") == []
        assert len(responses.calls) == 2

    @responses.activate
    def test_failover_on_connection_error(self, ha_client):
        responses.add(
            responses.GET, f"{NN1}/", body=requests.exceptions.ConnectionError("down"),
        )
        responses.add(
            responses.GET, f"{NN2}/", json={"FileStatuses": {"FileStatus": []}},
            status=200,
        )
        assert ha_client.listdir("/") == []

    @responses.activate
    def test_active_namenode_remembered(self, ha_client):
        responses.add(responses.GET, f"{NN1}/", json=STANDBY_BODY, status=403)
        responses.add(
            responses.GET, f"{NN2}/", json={"FileStatuses": {"FileStatus": []}},
            status=200,
        )
        ha_client.listdir("/")
        ha_client.listdir("/")
        # Second call should go straight to nn2 rather than retrying nn1.
        assert len(responses.calls) == 3
        assert ha_client.namenode_url.startswith("http://nn2")

    @responses.activate
    def test_all_namenodes_down(self, ha_client):
        for url in (f"{NN1}/", f"{NN2}/"):
            responses.add(
                responses.GET, url, body=requests.exceptions.ConnectionError("down"),
            )
        with pytest.raises(WebHDFSConnectionError, match="No active namenode"):
            ha_client.listdir("/")

    @responses.activate
    def test_standby_surfaces_when_single_host(self, client):
        """With one namenode there is nowhere to fail over to, so report the error."""
        responses.add(responses.GET, f"{BASE}/", json=STANDBY_BODY, status=403)
        with pytest.raises(WebHDFSRemoteException) as exc_info:
            client.listdir("/")
        assert exc_info.value.exception == "StandbyException"

    @responses.activate
    def test_real_error_not_retried(self, ha_client):
        """A genuine error is returned, not treated as a failover trigger."""
        responses.add(
            responses.GET, f"{NN1}/secret",
            json={"RemoteException": {"exception": "AccessControlException",
                                      "message": "Permission denied"}},
            status=403,
        )
        with pytest.raises(WebHDFSRemoteException):
            ha_client.listdir("/secret")
        assert len(responses.calls) == 1


# ------------------------------------------------------------------
# Misc
# ------------------------------------------------------------------

class TestMisc:
    @responses.activate
    def test_caller_params_not_mutated(self, client):
        """The client must not write auth params into a caller-visible dict."""
        responses.add(responses.PUT, f"{BASE}/d", json={"boolean": True}, status=200)
        params = {"op": "MKDIRS"}
        client._query(method="put", path="/d", params=params)
        assert params == {"op": "MKDIRS"}

    @responses.activate
    def test_boolean_params_lowercased(self, client):
        responses.add(responses.DELETE, f"{BASE}/d", json={"boolean": True}, status=200)
        client.remove("/d", recursive=False)
        assert "recursive=false" in responses.calls[0].request.url


# ------------------------------------------------------------------
# Write payload encoding
# ------------------------------------------------------------------

class TestWriteEncoding:
    """Text payloads must be encoded so Content-Length matches the body.

    ``requests`` derives Content-Length from a str's character count, so a
    multi-byte payload was silently truncated by the DataNode.
    """

    @staticmethod
    def _datanode_body(call):
        body = call.request.body
        return body if isinstance(body, bytes) else body.encode()

    @responses.activate
    def test_create_encodes_text(self, client):
        responses.add(
            responses.PUT, f"{BASE}/f.txt", status=307,
            headers={"Location": f"{DATANODE}/f.txt"},
        )
        responses.add(responses.PUT, f"{DATANODE}/f.txt", status=201)
        client.create("/f.txt", "café")

        request = responses.calls[1].request
        assert request.body == "café".encode()
        assert int(request.headers["Content-Length"]) == len("café".encode())

    @responses.activate
    def test_create_custom_encoding(self, client):
        responses.add(
            responses.PUT, f"{BASE}/f.txt", status=307,
            headers={"Location": f"{DATANODE}/f.txt"},
        )
        responses.add(responses.PUT, f"{DATANODE}/f.txt", status=201)
        client.create("/f.txt", "café", encoding="latin-1")
        assert responses.calls[1].request.body == "café".encode("latin-1")

    @responses.activate
    def test_create_passes_bytes_through(self, client):
        responses.add(
            responses.PUT, f"{BASE}/f.bin", status=307,
            headers={"Location": f"{DATANODE}/f.bin"},
        )
        responses.add(responses.PUT, f"{DATANODE}/f.bin", status=201)
        blob = bytes(range(256))
        client.create("/f.bin", blob)
        assert responses.calls[1].request.body == blob

    @responses.activate
    def test_append_encodes_text(self, client):
        responses.add(
            responses.POST, f"{BASE}/f.txt", status=307,
            headers={"Location": f"{DATANODE}/f.txt"},
        )
        responses.add(responses.POST, f"{DATANODE}/f.txt", status=200)
        client.append("/f.txt", "wörld")

        request = responses.calls[1].request
        assert request.body == "wörld".encode()
        assert int(request.headers["Content-Length"]) == len("wörld".encode())

    @responses.activate
    def test_copyfromlocal_streams_file_object(self, client, tmp_path):
        src = tmp_path / "in.bin"
        blob = bytes(range(256))
        src.write_bytes(blob)
        responses.add(
            responses.PUT, f"{BASE}/f.bin", status=307,
            headers={"Location": f"{DATANODE}/f.bin"},
        )
        responses.add(responses.PUT, f"{DATANODE}/f.bin", status=201)
        assert client.copyfromlocal(str(src), "/f.bin") is True
        assert self._datanode_body(responses.calls[1]) == blob
