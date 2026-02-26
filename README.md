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

### Available operations

- `listdir(path)` -- List directory contents
- `mkdir(path, permission=None)` -- Create directories
- `remove(path, recursive=False)` -- Delete files/directories
- `rename(src, dst)` -- Rename files/directories
- `open(path, offset=None, length=None, buffersize=None)` -- Read a file
- `create(path, file_data, overwrite=None)` -- Create a file
- `append(path, file_data, buffersize=None)` -- Append to a file
- `copyfromlocal(local_path, hdfs_path, overwrite=None)` -- Upload a local file
- `status(path)` -- Get file/directory status
- `chmod(path, permission)` -- Set permissions
- `set_owner(path, owner=None, group=None)` -- Set owner/group
- `set_replication(path, replication_factor)` -- Set replicaton factor
- `set_times(path, modificationtime=None, accesstime=None)` -- Set modification/access time
- `get_checksum(path)` -- Get file checksum
- `get_content_summary(path)` -- Get directory content summary
- `environ_home()` -- Get user home directory
- `get_delegation_token(renewer)` -- Get a delegation token
- `renew_delegation_token(token)` -- Renew a delegation token
- `cancel_delegation_token(token)` -- Cancel a delegation token

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
