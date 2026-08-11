"""A wrapper library to access Hadoop HTTP REST API."""
from __future__ import annotations

import logging
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any
from urllib.parse import quote

import requests

CONTEXT_ROOT = "/webhdfs/v1"
CHUNK_SIZE = 65536  # Default chunk size in bytes for streaming reads


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

    For an HA cluster, pass every namenode; the client transparently fails
    over to the next one when the active namenode is standby or unreachable::

        WebHDFSClient(["nn1.example.com", "nn2.example.com"], 9870)
    """

    def __init__(
        self,
        host: str | Sequence[str],
        port: int,
        username: str | None = None,
        logger: logging.Logger | None = None,
        *,
        timeout: float = 60.0,
        scheme: str = "http",
        auth: Any = None,
        verify: bool | str = True,
        cert: str | tuple[str, str] | None = None,
        session: requests.Session | None = None,
        token: str | None = None,
    ) -> None:
        """Create a new WebHDFS client.

        :param host: hostname of the HDFS namenode, or a sequence of
            hostnames to fail over between on an HA cluster
        :param port: port of the namenode(s)
        :param username: used for pseudo authentication (``user.name``)
        :param logger: optional logger instance
        :param timeout: request timeout in seconds
        :param scheme: URL scheme, ``"http"`` or ``"https"``
        :param auth: a ``requests`` auth handler, e.g.
            ``requests_kerberos.HTTPKerberosAuth()`` for a secured cluster
        :param verify: verify TLS certificates; may be a path to a CA bundle
        :param cert: client TLS certificate, as a path or a (cert, key) pair
        :param session: an existing :class:`requests.Session` to use; when
            given, the caller stays responsible for closing it
        :param token: a delegation token to authenticate with, used in
            preference to ``username``
        """
        self.hosts = [host] if isinstance(host, str) else list(host)
        if not self.hosts:
            raise WebHDFSException("At least one host must be specified")
        self.host = self.hosts[0]
        self.port = port
        self.username = username
        self.timeout = timeout
        self.token = token
        self.logger = logger or logging.getLogger(__name__)
        self._namenode_urls = [
            f"{scheme}://{h}:{port}{CONTEXT_ROOT}" for h in self.hosts
        ]
        self._active = 0
        self._owns_session = session is None
        self._session = session or requests.Session()
        if auth is not None:
            self._session.auth = auth
        if cert is not None:
            self._session.cert = cert
        self._session.verify = verify

    @property
    def namenode_url(self) -> str:
        """Base URL of the namenode currently believed to be active."""
        return self._namenode_urls[self._active]

    def set_delegation_token(self, token: str | None) -> None:
        """Authenticate subsequent requests with a delegation token.

        Pass ``None`` to stop sending a token.

        :param token: the ``urlString`` of a token from
            :meth:`get_delegation_token`
        """
        self.token = token

    def close(self) -> None:
        """Close the underlying HTTP session, unless it was supplied by the caller."""
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> WebHDFSClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _encode_path(path: str) -> str:
        """Percent-encode an HDFS path for use in a URL.

        HDFS permits characters such as ``?``, ``#``, ``%`` and spaces in
        filenames; without encoding they would be parsed as part of the query
        string or fragment and address the wrong file entirely.
        """
        if not path.startswith("/"):
            path = "/" + path
        return quote(path, safe="/")

    def _auth_params(self) -> dict[str, Any]:
        """Return the authentication query parameters for a request."""
        if self.token is not None:
            return {"delegation": self.token}
        if self.username is not None:
            return {"user.name": self.username}
        return {}

    def _send(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """Perform a single HTTP request, translating transport errors."""
        try:
            return self._session.request(
                method, url, timeout=self.timeout, **kwargs
            )
        except requests.RequestException as exc:
            raise WebHDFSConnectionError(
                f"Request to {url} failed: {exc}", cause=exc
            ) from exc

    @staticmethod
    def _remote_exception(response: requests.Response) -> dict[str, Any] | None:
        """Return the RemoteException body of an error response, if there is one."""
        try:
            remote = response.json()["RemoteException"]
        except (ValueError, KeyError, TypeError):
            return None
        return remote if isinstance(remote, dict) else None

    @classmethod
    def _is_standby(cls, response: requests.Response) -> bool:
        """Return whether the response says this namenode is in standby."""
        if response.status_code not in (403, 500):
            return False
        remote = cls._remote_exception(response)
        if remote is None:
            return False
        return "StandbyException" in str(remote.get("exception", ""))

    def _make_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any],
        allow_redirects: bool = False,
        **kwargs: Any,
    ) -> requests.Response:
        """Make an HTTP request to the namenode, failing over between hosts."""
        request_params = {**params, **self._auth_params()}
        encoded_path = self._encode_path(path)
        # Start from the last namenode known to be active, then try the rest.
        order = [
            (self._active + offset) % len(self._namenode_urls)
            for offset in range(len(self._namenode_urls))
        ]
        last_error: WebHDFSConnectionError | None = None
        for index in order:
            url = f"{self._namenode_urls[index]}{encoded_path}"
            try:
                response = self._send(
                    method,
                    url,
                    params=request_params,
                    allow_redirects=allow_redirects,
                    **kwargs,
                )
            except WebHDFSConnectionError as exc:
                self.logger.debug("Namenode %s unreachable: %s", self.hosts[index], exc)
                last_error = exc
                continue
            if self._is_standby(response) and len(order) > 1:
                self.logger.debug("Namenode %s is standby", self.hosts[index])
                last_error = WebHDFSConnectionError(
                    f"Namenode {self.hosts[index]} is in standby"
                )
                continue
            self._active = index
            return response
        assert last_error is not None  # the loop body always sets it before continuing
        if len(self.hosts) == 1:
            raise last_error
        raise WebHDFSConnectionError(
            f"No active namenode among {', '.join(self.hosts)}: {last_error}",
            cause=last_error.cause,
        )

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
        remote = WebHDFSClient._remote_exception(response)
        if remote is not None:
            raise WebHDFSRemoteException(
                message=remote.get("message", ""),
                status_code=response.status_code,
                exception=remote.get("exception", ""),
                java_class_name=remote.get("javaClassName", ""),
            )
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

    def _redirect_location(self, response: requests.Response, op: str) -> str:
        """Return the DataNode URL a namenode redirected to."""
        location = response.headers.get("location")
        if not location:
            raise WebHDFSException(
                f"NameNode did not return a redirect for {op}"
            )
        return location

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def listdir(self, path: str = "/") -> list[dict[str, Any]]:
        """List all the contents of a directory.

        :param path: path of the directory
        :returns: a list of FileStatus dicts
        """
        self.logger.debug("Listing %s", path)
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
        self.logger.debug("Creating directory %s", path)
        params: dict[str, Any] = {"op": "MKDIRS"}
        if permission is not None:
            params["permission"] = permission
        return self._query(method="put", path=path, params=params)

    def remove(self, path: str, recursive: bool = False) -> bool:
        """Delete a file or directory.

        :param path: path of the file or dir to delete
        :param recursive: delete content in subdirectories
        """
        self.logger.debug("Deleting %s", path)
        params: dict[str, Any] = {"op": "DELETE", "recursive": _bool(recursive)}
        return self._query(method="delete", path=path, params=params)

    def rename(self, src: str, dst: str) -> bool:
        """Rename a file or directory.

        :param src: path of the file or dir to rename
        :param dst: destination path
        """
        self.logger.debug("Renaming %s", src)
        params: dict[str, Any] = {"op": "RENAME", "destination": dst}
        return self._query(method="put", path=src, params=params)

    # ------------------------------------------------------------------
    # File read operations
    # ------------------------------------------------------------------

    def environ_home(self) -> str:
        """Return the home directory of the user."""
        self.logger.debug("Getting environment home")
        params: dict[str, Any] = {"op": "GETHOMEDIRECTORY"}
        return self._query(method="get", path="/", params=params, json_path=["Path"])

    def _open_params(
        self,
        offset: int | None,
        length: int | None,
        buffersize: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"op": "OPEN"}
        if offset is not None:
            params["offset"] = offset
        if length is not None:
            params["length"] = length
        if buffersize is not None:
            params["buffersize"] = buffersize
        return params

    def open(self, path: str, offset: int | None = None, length: int | None = None,
             buffersize: int | None = None, encoding: str = "utf-8") -> str:
        """Open a text file and return its contents as a string.

        Use :meth:`read` for binary files and :meth:`stream` or
        :meth:`copytolocal` for files too large to hold in memory.

        :param path: path of the file
        :param offset: starting byte position
        :param length: number of bytes to read
        :param buffersize: size of the buffer used to transfer the data
        :param encoding: codec used to decode the data
        :returns: the file data as text
        """
        return self.read(path, offset, length, buffersize).decode(encoding)

    def read(self, path: str, offset: int | None = None, length: int | None = None,
             buffersize: int | None = None) -> bytes:
        """Read a file and return its contents as bytes.

        :param path: path of the file
        :param offset: starting byte position
        :param length: number of bytes to read
        :param buffersize: size of the buffer used to transfer the data
        :returns: the file data as bytes
        """
        self.logger.debug("Reading %s", path)
        r = self._make_request(
            method="get",
            path=path,
            params=self._open_params(offset, length, buffersize),
            allow_redirects=True,
        )
        self._check_response(r)
        return r.content

    @contextmanager
    def stream(self, path: str, offset: int | None = None, length: int | None = None,
               buffersize: int | None = None,
               chunk_size: int = CHUNK_SIZE) -> Iterator[Iterator[bytes]]:
        """Stream a file's contents without holding it all in memory.

        Yields an iterator of byte chunks::

            with client.stream("/big.bin") as chunks:
                for chunk in chunks:
                    process(chunk)

        :param path: path of the file
        :param offset: starting byte position
        :param length: number of bytes to read
        :param buffersize: size of the buffer used to transfer the data
        :param chunk_size: number of bytes yielded per chunk
        """
        self.logger.debug("Streaming %s", path)
        r = self._make_request(
            method="get",
            path=path,
            params=self._open_params(offset, length, buffersize),
            allow_redirects=True,
            stream=True,
        )
        try:
            self._check_response(r)
            yield r.iter_content(chunk_size=chunk_size)
        finally:
            r.close()

    def copytolocal(self, hdfs_path: str, local_path: str,
                    chunk_size: int = CHUNK_SIZE) -> bool:
        """Download a file from HDFS to the local filesystem.

        The file is streamed, so its size is not limited by available memory.

        :param hdfs_path: path of the HDFS file
        :param local_path: local destination path
        :param chunk_size: number of bytes transferred per chunk
        """
        self.logger.debug("Copying %s to local file %s", hdfs_path, local_path)
        with self.stream(hdfs_path, chunk_size=chunk_size) as chunks, \
                open(local_path, "wb") as writer:
            for chunk in chunks:
                writer.write(chunk)
        return True

    def status(self, path: str) -> dict[str, Any]:
        """Return the FileStatus of a file or directory.

        :param path: path of the file/dir
        :returns: a FileStatus dictionary
        """
        self.logger.debug("Getting status of %s", path)
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
        self.logger.debug("Getting checksum of %s", path)
        params: dict[str, Any] = {"op": "GETFILECHECKSUM"}
        r = self._make_request(method="get", path=path, params=params)
        self._check_response(r, {307})
        location = self._redirect_location(r, "GETFILECHECKSUM")
        r = self._send("get", location)
        self._check_response(r)
        return r.json()["FileChecksum"]

    def get_content_summary(self, path: str) -> dict[str, Any]:
        """Return the content summary of a directory.

        :param path: path of the directory
        :returns: ContentSummary dict
        """
        self.logger.debug("Getting content summary of %s", path)
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

    def create(self, path: str, file_data: Any, overwrite: bool | None = None,
               encoding: str = "utf-8") -> bool:
        """Create a new file in HDFS.

        Uses the two-step WebHDFS create protocol (NameNode redirect then
        DataNode upload).

        :param path: the file path to create
        :param file_data: the data to write, as text, bytes or a file object
        :param overwrite: whether to overwrite an existing file
        :param encoding: codec used to encode ``file_data`` when it is text
        """
        self.logger.debug("Creating %s", path)
        params: dict[str, Any] = {"op": "CREATE"}
        if overwrite is not None:
            params["overwrite"] = _bool(overwrite)
        r = self._make_request(method="put", path=path, params=params,
                               allow_redirects=False)
        self._check_response(r, {307})
        location = self._redirect_location(r, "CREATE")
        r = self._send(
            "put",
            location,
            data=_encode_body(file_data, encoding),
            headers={"content-type": "application/octet-stream"},
        )
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
        self.logger.debug("Copying local file %s to %s", local_path, hdfs_path)
        if not os.path.exists(local_path):
            raise WebHDFSException(f"The local file {local_path} doesn't exist")
        with open(local_path, "rb") as reader:
            return self.create(hdfs_path, reader, overwrite=overwrite)

    def append(self, path: str, file_data: Any,
               buffersize: int | None = None, encoding: str = "utf-8") -> bool:
        """Append data to a file.

        :param path: path of the file
        :param file_data: data to append, as text, bytes or a file object
        :param buffersize: size of the buffer used to transfer the data
        :param encoding: codec used to encode ``file_data`` when it is text
        """
        self.logger.debug("Appending to file %s", path)
        params: dict[str, Any] = {"op": "APPEND"}
        if buffersize is not None:
            params["buffersize"] = buffersize
        r = self._make_request(method="post", path=path, params=params)
        self._check_response(r, {307})
        location = self._redirect_location(r, "APPEND")
        r = self._send("post", location, data=_encode_body(file_data, encoding))
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
        self.logger.debug("Setting permissions of %s to %s", path, permission)
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
        self.logger.debug("Setting owner of %s", path)
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
        self.logger.debug(
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
        self.logger.debug("Setting times of %s", path)
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

        Pass the token's ``urlString`` to :meth:`set_delegation_token` (or the
        ``token`` constructor argument) to authenticate with it.

        :param renewer: the user who can renew the token
        :returns: Token dict
        """
        self.logger.debug("Getting delegation token for renewer %s", renewer)
        params: dict[str, Any] = {"op": "GETDELEGATIONTOKEN", "renewer": renewer}
        return self._query(
            method="get", path="/", params=params, json_path=["Token"]
        )

    def renew_delegation_token(self, token: str) -> int:
        """Renew a delegation token.

        :param token: the delegation token
        :returns: new expiration time in ms since epoch
        """
        self.logger.debug("Renewing delegation token")
        params: dict[str, Any] = {"op": "RENEWDELEGATIONTOKEN", "token": token}
        return self._query(
            method="put", path="/", params=params, json_path=["long"]
        )

    def cancel_delegation_token(self, token: str) -> bool:
        """Cancel a delegation token.

        :param token: the delegation token
        """
        self.logger.debug("Cancelling delegation token")
        params: dict[str, Any] = {"op": "CANCELDELEGATIONTOKEN", "token": token}
        return self._query(method="put", path="/", json_path=[], params=params)


def _bool(value: bool) -> str:
    """Render a boolean the way the WebHDFS API documents it."""
    return "true" if value else "false"


def _encode_body(file_data: Any, encoding: str) -> Any:
    """Encode a text payload to bytes, leaving other payload types alone.

    ``requests`` sends a ``str`` body as-is but derives ``Content-Length`` from
    its *character* count, so a payload containing any multi-byte character is
    silently truncated by the DataNode. Encoding it ourselves keeps the header
    and the body in agreement.
    """
    if isinstance(file_data, str):
        return file_data.encode(encoding)
    return file_data
