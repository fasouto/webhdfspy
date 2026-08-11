# webhdfspy

A Python wrapper library to access the [Hadoop WebHDFS REST API](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).

## Installation

```bash
pip install webhdfspy
```

## Python versions

webhdfspy requires Python 3.9+

## Usage

```python
import webhdfspy

# Basic usage
client = webhdfspy.WebHDFSClient("localhost", 50070, "username")
print(client.listdir("/"))
client.mkdir("/foo")
client.create("/foo/foo.txt", "just put some text here", overwrite=True)
print(client.open("/foo/foo.txt"))
client.remove("/foo", recursive=True)
client.close()

# Context manager (recommended)
with webhdfspy.WebHDFSClient("localhost", 50070, "username") as client:
    client.mkdir("/data")
    client.create("/data/hello.txt", "Hello, HDFS!", overwrite=True)
    print(client.open("/data/hello.txt"))

# HTTPS support
with webhdfspy.WebHDFSClient("host", 9871, "user", scheme="https") as client:
    print(client.listdir("/"))

# Custom timeout (default: 60s)
client = webhdfspy.WebHDFSClient("host", 50070, timeout=30.0)
```

### Binary and large files

`open()` returns text. For binary data use `read()`, and for files too large to
hold in memory use `stream()` or `copytolocal()`:

```python
data = client.read("/data/image.png")           # bytes, never re-encoded

with client.stream("/data/huge.parquet") as chunks:
    for chunk in chunks:
        process(chunk)

client.copytolocal("/data/huge.parquet", "local.parquet")
```

### High availability

Pass every namenode and the client fails over when the active one is standby or
unreachable, remembering which namenode answered:

```python
client = webhdfspy.WebHDFSClient(["nn1.example.com", "nn2.example.com"], 9870)
```

### Secured clusters

`auth` takes any `requests` auth handler, and `verify`/`cert` control TLS:

```python
from requests_kerberos import HTTPKerberosAuth  # pip install webhdfspy[kerberos]

client = webhdfspy.WebHDFSClient(
    "host", 9871, scheme="https",
    auth=HTTPKerberosAuth(),
    verify="/etc/pki/ca-trust/ca-bundle.crt",
)

# Or authenticate with a delegation token
token = client.get_delegation_token("renewer")
client.set_delegation_token(token["urlString"])
```

### Available operations

| Method | Description |
|--------|-------------|
| `listdir(path)` | List directory contents |
| `mkdir(path, permission=None)` | Create directories |
| `remove(path, recursive=False)` | Delete files/directories |
| `rename(src, dst)` | Rename files/directories |
| `open(path, offset=None, length=None, buffersize=None, encoding="utf-8")` | Read a file as text |
| `read(path, offset=None, length=None, buffersize=None)` | Read a file as bytes |
| `stream(path, ..., chunk_size=65536)` | Stream a file in chunks |
| `create(path, file_data, overwrite=None, encoding="utf-8")` | Create a file |
| `append(path, file_data, buffersize=None, encoding="utf-8")` | Append to a file |
| `copyfromlocal(local_path, hdfs_path, overwrite=None)` | Upload a local file |
| `copytolocal(hdfs_path, local_path, chunk_size=65536)` | Download to a local file |
| `status(path)` | Get file/directory status |
| `chmod(path, permission)` | Set permissions |
| `set_owner(path, owner=None, group=None)` | Set owner/group |
| `set_replication(path, replication_factor)` | Set replication factor |
| `set_times(path, modificationtime=None, accesstime=None)` | Set modification/access time |
| `get_checksum(path)` | Get file checksum |
| `get_content_summary(path)` | Get directory content summary |
| `environ_home()` | Get user home directory |
| `get_delegation_token(renewer)` | Get a delegation token |
| `set_delegation_token(token)` | Authenticate with a delegation token |
| `renew_delegation_token(token)` | Renew a delegation token |
| `cancel_delegation_token(token)` | Cancel a delegation token |

## Development

```bash
pip install -e ".[dev]"
pytest                  # unit tests
ruff check . && mypy    # lint and type checks
```

The integration tests need a real cluster. There is a single-node one in
`tests/docker-compose.yml`:

```bash
docker compose -f tests/docker-compose.yml up -d
echo "127.0.0.1 datanode" | sudo tee -a /etc/hosts   # WebHDFS redirects by hostname
pytest tests/test_integration.py --integration
```

Point them at your own cluster with `WEBHDFSPY_TEST_HOST`,
`WEBHDFSPY_TEST_PORT`, `WEBHDFSPY_TEST_USER` and `WEBHDFSPY_TEST_DIR`.
Everything under the test directory (`/webhdfspy_test` by default) is deleted.

## Documentation

http://webhdfspy.readthedocs.org/en/latest/

## Hadoop configuration

To enable WebHDFS in Hadoop, add this to your `$HADOOP_DIR/conf/hdfs-site.xml`:

```xml
<property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
</property>
```

To enable append on HDFS:

```xml
<property>
    <name>dfs.support.append</name>
    <value>true</value>
</property>
```

More about WebHDFS: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
