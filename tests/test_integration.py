"""
Warning: these tests will destroy everything under /pywebhdfs_test.
Modify TEST_DIR_PATH if you want to change this.

Requires a running Hadoop cluster with WebHDFS enabled.
Run with: pytest tests/test_integration.py --integration
"""
import os

import pytest

import webhdfspy

pytestmark = pytest.mark.integration

TEST_DIR_PATH = "/pywebhdfs_test"
TEST_DIR_PARENT = os.path.abspath(os.path.join(TEST_DIR_PATH, os.pardir))
TEST_DIR = os.path.basename(TEST_DIR_PATH)
HADOOP_USERNAME = "fabio"


@pytest.fixture()
def client():
    with webhdfspy.WebHDFSClient("localhost", 50070, HADOOP_USERNAME) as c:
        c.mkdir(TEST_DIR_PATH)
        yield c
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

    def test_append(self, client):
        client.create(TEST_DIR_PATH + "/barfoo.txt", "foo", overwrite=True)
        client.append(TEST_DIR_PATH + "/barfoo.txt", "bar")
        file_data = client.open(TEST_DIR_PATH + "/barfoo.txt")
        assert file_data == "foobar"


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
        assert checksum["bytes"] == (
            "00000200000000000000000043d7180b6d1dfa6acae636572cd3b70f00000000"
        )
        assert checksum["length"] == 28
