"""A wrapper library to access Hadoop HTTP REST API."""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import requests

CONTEXT_ROOT = "/webhdfs/v1"
OFFSET = 32768  # Default offset in bytes


class WebHDFSException(Exception):
    """Base exception for WebHDFS errors."""

    def __init__(self, msg: str) -> None:
        self.msg = msg
        super().__init__(msg)

    def __str__(self) -> str:
        return self.msg


class WebHDFSRemoteException(WebHDFSException):
    """Exception raised when WebHDFS returns a RemoteException."""

    def __init__(
        self,
        message: str,
        status_code: int,
        exception: str = "",
        java_class_name: str = "",
    ) -> None:
        self.status_code = status_code
        self.exception = exception
        self.java_class_name = java_class_name
        super().__init__(message)


class WebHDFSConnectionError(WebHDFSException):
    """Exception raised when a connection to WebHDFS fails."""

    def __init__(self, msg: str, cause: Exception | None = None) -> None:
        self.cause = cause
        super().__init__(msg)


class WebHDFSClient:
    """Client for Hadoop WebHDFS REST API.

    Supports context manager protocol for automatic resource cleanup::

        with WebHDFSClient("host", 50070, username="user") as client:
            client.listdir("/")
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str | None = None,
        logger: logging.Logger | None = None,
        *,
        timeout: float = 60.0,
        scheme: str = "http",
    ) -> None:
        """Create a new WebHDFS client.

        :param host: hostname of the HDFS namenode
        :param port: port of the namenode
        :param username: used for authentication
        :param logger: optional logger instance
        :param timeout: request timeout in seconds
        :param scheme: URL scheme, ``"http"`` or ``"https"``
        """
        self.host = host
        self.port = port
        self.username = username
        self.timeout = timeout
        self.namenode_url = f"{scheme}://{host}:{port}{CONTEXT_ROOT}"
        self.logger = logger or logging.getLogger(__name__)
        self._session = requests.Session()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> WebHDFSClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any],
        allow_redirects: bool = False,
    ) -> requests.Response:
        """Make an HTTP request to the namenode."""
        if self.username is not None:
            params["user.name"] = self.username
        try:
            return self._session.request(
                method,
                f"{self.namenode_url}{path}",
                params=params,
                allow_redirects=allow_redirects,
                timeout=self.timeout,
            )
        except requests.ConnectionError as exc:
            raise WebHDFSConnectionError(
                f"Failed to connect to {self.host}:{self.port}", cause=exc
            ) from exc

    @staticmethod
    def _check_response(
        response: requests.Response,
        expected_status: set[int] | None = None,
    ) -> None:
        """Raise an appropriate exception if the response indicates an error."""
        if expected_status is None:
            expected_status = {200}
        if response.status_code in expected_status:
            return
        # Try to parse WebHDFS RemoteException
        try:
            body = response.json()
            remote = body["RemoteException"]
            raise WebHDFSRemoteException(
                message=remote.get("message", ""),
                status_code=response.status_code,
                exception=remote.get("exception", ""),
                java_class_name=remote.get("javaClassName", ""),
            )
        except (ValueError, KeyError, TypeError):
            pass
        text = response.text[:500] if response.text else ""
        raise WebHDFSException(
            f"WebHDFS request failed with status {response.status_code}: {text}"
        )

    def _query(
        self,
        method: str,
        path: str,
        params: dict[str, Any],
        json_path: list[str] | None = None,
        allow_redirects: bool = False,
        expected_status: set[int] | None = None,
    ) -> Any:
        """Make a request and extract a value from the JSON response."""
        if json_path is None:
            json_path = ["boolean"]
        r = self._make_request(method, path, params, allow_redirects)
        self._check_response(r, expected_status)
        if json_path:
            response = r.json()
            for key in json_path:
                response = response[key]
            return response
        return True

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def listdir(self, path: str = "/") -> list[dict[str, Any]]:
        """List all the contents of a directory.

        :param path: path of the directory
        :returns: a list of FileStatus dicts
        """
        self.logger.info("Listing %s", path)
        params = {"op": "LISTSTATUS"}
        return self._query(
            method="get",
            path=path,
            params=params,
            json_path=["FileStatuses", "FileStatus"],
        )

    def mkdir(self, path: str, permission: str | None = None) -> bool:
        """Create a directory hierarchy, like ``mkdir -p``.

        :param path: the path of the directory
        :param permission: dir permissions in octal (e.g. ``"755"``)
        """
        self.logger.info("Creating directory %s", path)
        params: dict[str, Any] = {"op": "MKDIRS"}
        if permission is not None:
            params["permission"] = permission
        return self._query(method="put", path=path, params=params)

    def remove(self, path: str, recursive: bool = False) -> bool:
        """Delete a file or directory.

        :param path: path of the file or dir to delete
        :param recursive: delete content in subdirectories
        """
        self.logger.info("Deleting %s", path)
        params: dict[str, Any] = {"op": "DELETE", "recursive": recursive}
        return self._query(method="delete", path=path, params=params)

    def rename(self, src: str, dst: str) -> bool:
        """Rename a file or directory.

        :param src: path of the file or dir to rename
        :param dst: destination path
        """
        self.logger.info("Renaming %s", src)
        params: dict[str, Any] = {"op": "RENAME", "destination": dst}
        return self._query(method="put", path=src, params=params)

    # ------------------------------------------------------------------
    # File read operations
    # ------------------------------------------------------------------

    def environ_home(self) -> str:
        """Return the home directory of the user."""
        self.logger.info("Getting environment home")
        params: dict[str, Any] = {"op": "GETHOMEDIRECTORY"}
        return self._query(method="get", path="/", params=params, json_path=["Path"])

    def open(self, path: str, offset: int | None = None, length: int | None = None,
             buffersize: int | None = None) -> str:
        """Open a file to read.

        :param path: path of the file
        :param offset: starting byte position
        :param length: number of bytes to read
        :param buffersize: size of the buffer used to transfer the data
        :returns: the file data as text
        """
        self.logger.info("Opening %s", path)
        params: dict[str, Any] = {"op": "OPEN"}
        if offset is not None:
            params["offset"] = offset
        if length is not None:
            params["length"] = length
        if buffersize is not None:
            params["buffersize"] = buffersize
        r = self._make_request(method="get", path=path, params=params,
                               allow_redirects=True)
        self._check_response(r)
        return r.text

    def status(self, path: str) -> dict[str, Any]:
        """Return the FileStatus of a file or directory.

        :param path: path of the file/dir
        :returns: a FileStatus dictionary
        """
        self.logger.info("Getting status of %s", path)
        params: dict[str, Any] = {"op": "GETFILESTATUS"}
        return self._query(
            method="get",
            path=path,
            params=params,
            json_path=["FileStatus"],
            allow_redirects=True,
        )

    def get_checksum(self, path: str) -> dict[str, Any]:
        """Return the checksum of a file.

        :param path: path of the file
        :returns: FileChecksum dict
        """
        self.logger.info("Getting checksum of %s", path)
        params: dict[str, Any] = {"op": "GETFILECHECKSUM"}
        r = self._make_request(method="get", path=path, params=params)
        self._check_response(r, {307})
        location = r.headers.get("location")
        if not location:
            raise WebHDFSException(
                "NameNode did not return a redirect for GETFILECHECKSUM"
            )
        try:
            r = self._session.get(location, timeout=self.timeout)
        except requests.ConnectionError as exc:
            raise WebHDFSConnectionError(
                "Failed to connect to DataNode for checksum", cause=exc
            ) from exc
        self._check_response(r)
        return r.json()["FileChecksum"]

    def get_content_summary(self, path: str) -> dict[str, Any]:
        """Return the content summary of a directory.

        :param path: path of the directory
        :returns: ContentSummary dict
        """
        self.logger.info("Getting content summary of %s", path)
        params: dict[str, Any] = {"op": "GETCONTENTSUMMARY"}
        return self._query(
            method="get",
            path=path,
            params=params,
            json_path=["ContentSummary"],
        )

    # ------------------------------------------------------------------
    # File write operations
    # ------------------------------------------------------------------

    def create(self, path: str, file_data: Any, overwrite: bool | None = None) -> bool:
        """Create a new file in HDFS.

        Uses the two-step WebHDFS create protocol (NameNode redirect then
        DataNode upload).

        :param path: the file path to create
        :param file_data: the data to write
        :param overwrite: whether to overwrite an existing file
        """
        self.logger.info("Creating %s", path)
        params: dict[str, Any] = {"op": "CREATE"}
        if overwrite is not None:
            params["overwrite"] = overwrite
        r = self._make_request(method="put", path=path, params=params,
                               allow_redirects=False)
        self._check_response(r, {307})
        location = r.headers.get("location")
        if not location:
            raise WebHDFSException("NameNode did not return a redirect for CREATE")
        try:
            r = self._session.put(
                location,
                data=file_data,
                headers={"content-type": "application/octet-stream"},
                timeout=self.timeout,
            )
        except requests.ConnectionError as exc:
            raise WebHDFSConnectionError(
                "Failed to connect to DataNode for create", cause=exc
            ) from exc
        self._check_response(r, {201})
        return True

    def copyfromlocal(
        self, local_path: str, hdfs_path: str, overwrite: bool | None = None
    ) -> bool:
        """Copy a file from the local filesystem to HDFS.

        :param local_path: path of the local file
        :param hdfs_path: HDFS destination path
        :param overwrite: whether to overwrite an existing file
        """
        self.logger.info("Copying local file %s to %s", local_path, hdfs_path)
        if not os.path.exists(local_path):
            raise WebHDFSException(f"The local file {local_path} doesn't exist")
        with open(local_path, "rb") as reader:
            return self.create(hdfs_path, reader, overwrite=overwrite)

    def append(self, path: str, file_data: Any,
               buffersize: int | None = None) -> bool:
        """Append data to a file.

        :param path: path of the file
        :param file_data: data to append
        :param buffersize: size of the buffer used to transfer the data
        """
        self.logger.info("Appending to file %s", path)
        params: dict[str, Any] = {"op": "APPEND"}
        if buffersize is not None:
            params["buffersize"] = buffersize
        r = self._make_request(method="post", path=path, params=params)
        self._check_response(r, {307})
        location = r.headers.get("location")
        if not location:
            raise WebHDFSException("NameNode did not return a redirect for APPEND")
        try:
            r = self._session.post(
                location,
                data=file_data,
                timeout=self.timeout,
            )
        except requests.ConnectionError as exc:
            raise WebHDFSConnectionError(
                "Failed to connect to DataNode for append", cause=exc
            ) from exc
        self._check_response(r)
        return True

    # ------------------------------------------------------------------
    # Permission / ownership operations
    # ------------------------------------------------------------------

    def chmod(self, path: str, permission: str) -> bool:
        """Set the permissions of a file or directory.

        :param path: path of the file/dir
        :param permission: permissions in octal (e.g. ``"755"``)
        """
        self.logger.info("Setting permissions of %s to %s", path, permission)
        params: dict[str, Any] = {"op": "SETPERMISSION", "permission": permission}
        return self._query(method="put", path=path, json_path=[], params=params)

    def set_owner(
        self,
        path: str,
        owner: str | None = None,
        group: str | None = None,
    ) -> bool:
        """Set the owner and/or group of a file or directory.

        :param path: path of the file/dir
        :param owner: new owner name
        :param group: new group name
        """
        if owner is None and group is None:
            raise WebHDFSException("At least one of owner or group must be specified")
        self.logger.info("Setting owner of %s", path)
        params: dict[str, Any] = {"op": "SETOWNER"}
        if owner is not None:
            params["owner"] = owner
        if group is not None:
            params["group"] = group
        return self._query(method="put", path=path, json_path=[], params=params)

    def set_replication(self, path: str, replication_factor: int) -> bool:
        """Set the replication factor of a file.

        :param path: path of the file
        :param replication_factor: number of replications (>0)
        """
        self.logger.info(
            "Setting replication factor of %s to %s", path, replication_factor
        )
        params: dict[str, Any] = {
            "op": "SETREPLICATION",
            "replication": replication_factor,
        }
        return self._query(method="put", path=path, params=params)

    def set_times(
        self,
        path: str,
        modificationtime: int | None = None,
        accesstime: int | None = None,
    ) -> bool:
        """Set modification and/or access time of a file.

        :param path: path of the file
        :param modificationtime: modification time in ms since epoch
        :param accesstime: access time in ms since epoch
        """
        self.logger.info("Setting times of %s", path)
        params: dict[str, Any] = {"op": "SETTIMES"}
        if modificationtime is not None:
            params["modificationtime"] = modificationtime
        if accesstime is not None:
            params["accesstime"] = accesstime
        return self._query(method="put", path=path, json_path=[], params=params)

    # ------------------------------------------------------------------
    # Delegation token operations
    # ------------------------------------------------------------------

    def get_delegation_token(self, renewer: str) -> dict[str, Any]:
        """Get a delegation token.

        :param renewer: the user who can renew the token
        :returns: Token dict
        """
        self.logger.info("Getting delegation token for renewer %s", renewer)
        params: dict[str, Any] = {"op": "GETDELEGATIONTOKEN", "renewer": renewer}
        return self._query(
            method="get", path="/", params=params, json_path=["Token"]
        )

    def renew_delegation_token(self, token: str) -> int:
        """Renew a delegation token.

        :param token: the delegation token
        :returns: new expiration time in ms since epoch
        """
        self.logger.info("Renewing delegation token")
        params: dict[str, Any] = {"op": "RENEWDELEGATIONTOKEN", "token": token}
        return self._query(
            method="put", path="/", params=params, json_path=["long"]
        )

    def cancel_delegation_token(self, token: str) -> bool:
        """Cancel a delegation token.

        :param token: the delegation token
        """
        self.logger.info("Cancelling delegation token")
        params: dict[str, Any] = {"op": "CANCELDELEGATIONTOKEN", "token": token}
        return self._query(method="put", path="/", json_path=[], params=params)
