# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Paths are now percent-encoded.** Filenames containing `?`, `#`, `%`, spaces
  or non-ASCII characters were interpolated straight into the request URL, so
  they addressed the wrong file. `#` was the worst case: `remove("/data/rep#1")`
  silently deleted `/data/rep` instead, because everything from the `#` was
  parsed as a URL fragment.
- **Text writes are no longer truncated.** `requests` derives `Content-Length`
  from a `str`'s character count, so `create()` and `append()` silently dropped
  the trailing bytes of any payload containing multi-byte characters. Text is
  now encoded before it is sent, and `create()`/`append()` take an `encoding`
  argument.
- **Read timeouts are wrapped.** Only `requests.ConnectionError` was caught, but
  `ReadTimeout` does not inherit from it, so a timeout during the read phase
  leaked a raw `requests` exception. All transport failures now raise
  `WebHDFSConnectionError`, with the original exception on `.cause`.
- `open()` decodes with an explicit codec (UTF-8 by default) instead of relying
  on charset guessing.
- The client no longer writes authentication parameters into the caller's dict.

### Added

- `read()` returns file contents as `bytes`, leaving binary files intact.
- `stream()` yields a file in chunks, and `copytolocal()` downloads to a local
  path, so files larger than memory can be read.
- HA support: pass several namenodes (`WebHDFSClient(["nn1", "nn2"], 9870)`) and
  the client fails over on `StandbyException` or a connection failure,
  remembering which namenode answered.
- `auth`, `verify` and `cert` arguments for Kerberos/SPNEGO and TLS clusters,
  and `session` to supply a preconfigured `requests.Session`.
- Delegation tokens are now usable: pass `token=` or call
  `set_delegation_token()` and requests authenticate with `delegation=`.
- `py.typed` marker, so the annotations are visible to downstream type checkers.
- Continuous integration running lint, type checks, and the unit tests on
  Python 3.9-3.13, plus the integration suite against a real HDFS cluster.

### Changed

- Boolean query parameters are sent as `true`/`false` per the WebHDFS docs,
  rather than Python's `True`/`False`.
- Per-operation logging moved from `INFO` to `DEBUG`, which is the usual
  convention for a library.
- The integration tests read their cluster configuration from
  `WEBHDFSPY_TEST_HOST`, `WEBHDFSPY_TEST_PORT`, `WEBHDFSPY_TEST_USER` and
  `WEBHDFSPY_TEST_DIR` instead of hardcoding a username.

### Removed

- The vendored copy of the alabaster Sphinx theme; it is now an ordinary
  documentation dependency.

## [1.0.0]

- Modernised packaging, type annotations, and the exception hierarchy.
- Added context manager support, configurable timeout, and HTTPS.
