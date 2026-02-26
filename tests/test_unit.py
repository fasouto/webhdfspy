"""Unit tests for webhdfspy using the ``responses`` library to mock HTTP."""
import json

import pytest
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
        assert "recursive=True" in responses.calls[0].request.url


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
        assert "overwrite=True" in responses.calls[0].request.url


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
        assert client.set_times("/file.txt", modificationtime=1000, accesstime=2000) is True
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
