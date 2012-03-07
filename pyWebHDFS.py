#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""A wrapper library to access Hadoop HTTP REST API"""

__author__ = 'fsoutomoure@gmail.com'
__version__ = '0.1'

import requests
try:
    import json  # Python >= 2.6
except ImportError:
    try:
        import simplejson as json  # Python < 2.6
    except ImportError:
        try:
            from django.utils import simplejson as json
        except ImportError:
            raise ImportError("Unable to load a json library")

CONTEXT_ROOT = '/webhdfs/v1'
OFFSET = 32768  # Default offset in bytes


class pyWebHDFSException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class pyWebHDFS(object):
    """
    TODO:
     authentication
     copytolocal
     test with big files, this is big data
    """
    def __init__(self, host, port, username):
        self.host = host
        self.port = port
        self.user = username
        self.namenode_url = 'http://%s:%s%s' % (host, port, CONTEXT_ROOT)

    def listdir(self, path):
        """
        List all the contents of a directory

        @param path path of the directory
        @return a list of fileStatusProperties:
        http://hadoop.apache.org/common/docs/r1.0.0/webhdfs.html#fileStatusProperties False on error
        """
        params = {'op': 'LISTSTATUS'}
        r = requests.get("%s%s" % (self.namenode_url, path), params=params)
        if r.status_code == 200:
            r_data = json.loads(r.text)
            return r_data['FileStatuses']['FileStatus']
        else:
            return False

    def mkdir(self, path, permission=None):
        """
        Create a directory hierarchy, like the unix command mkdir -p

        @param path the path of the directory
        @param permission dir permissions in octal (0-777)
        @return True on success, if not False
        """
        params = {'op': 'MKDIRS', 'permission': permission}
        r = requests.put("%s%s" % (self.namenode_url, path), params=params)
        if r.status_code == 200:
            return True
        return False

    def remove(self, path, recursive=None):
        """
        Delete a file o directory

        @param path path of the file or dir to delete
        @param recursive true to delete the content in subdirectories
        @return true on success, otherwise false
        """
        params = {'op': 'DELETE', 'recursive': recursive}
        r = requests.delete("%s%s" % (self.namenode_url, path), params=params)
        if r.status_code == 200:
            return True
        return False

    def rename(self, src, dst):
        """
        Rename a file or directory

        @param src path of the file or dir to rename
        @param dst path of the final file/dir
        @return true on success, false if error
        """
        params = {'op': 'RENAME', 'destination': dst}
        r = requests.put("%s%s" % (self.namenode_url, src), params=params)
        if r.status_code == 200:
            return True
        return False

    def environ_home(self):
        """
        @return the home directory of the user
        """
        params = {'op': 'GETHOMEDIRECTORY'}
        r = requests.get('%s' % self.namenode_url, params=params)
        r_data = json.loads(r.text)
        return r_data['Path']

    def open(self, path, offset=None, length=None, buffersize=None):
        params = {'op': 'OPEN', 'offset': offset, 'length': length, 'buffersize': buffersize}
        r = requests.get('%s%s' % (self.namenode_url, path), params=params)
        return r.text

    def status(self, path):
        """
        Status of a file/dir

        @param path path of the file/dir
        @return a FileStatus dictionary on success, false otherwise
        """
        params = {'op': 'GETFILESTATUS'}
        r = requests.get('%s%s' % (self.namenode_url, path), params=params, allow_redirects=True)
        if r.status_code == 200:
            r_data = json.loads(r.text)
            return  r_data['FileStatus']
        return False

    def chmod(self, path, permission):
        """
        Set the permissions

        @param path path of the file/dir
        @param permission dir permissions in octal (0-777)
        @return True on success, false otherwise
        """
        params = {'op': 'SETPERMISSION', 'permission': permission}
        r = requests.put('%s%s' % (self.namenode_url, path), params=params)
        if r.status_code == 200:
            return True
        return False

    # def copyToLocal(self, hdfs_path, local_path, offset=None, length=None, buffersize=None):
    #   """
    #   Copy a file from HDFS to the local filesystem

    #   @param
    #   """
    #   params = {"op":"OPEN", "offset":offset, "length":length, "buffersize":buffersize}
    #   r = requests.get("%s%s" % (self.namenode_url, hdfs_path), params=params)
    #   localfile = open(local_path, "w")
    #   localfile.write(r.text)
    #   localfile.close()

    # def create(self, path, overwrite=None):
    #   """FIXIT"""
    #   params = {'op':'CREATE','overwrite':overwrite}
    #   # r = requests.put("%s%s" % (WEBDFS_ROOT, path), params=params, allow_redirects=True)
    #   r = requests.put("http://localhost:50070/webhdfs/v1%s" % path , params=params)
    #   if r.status_code == 403:
    #       print "403: The file already exists"
    #   url_location = r.headers['location']
    #   return url_location
    #   r = requests.put(url_location, data = "aaaaaaaaaw yeah2!")
    #   print r.headers

    # def read_in_chunks(self, file_object, chunk_size=1024):
    #     """Lazy function (generator) to read a file piece by piece.
    #     Default chunk size: 1k."""
    #     while True:
    #         data = file_object.read(chunk_size)
    #         if not data:
    #             break
    #         yield data

    # f = open('really_big_file.dat')
    # for piece in read_in_chunks(f):
    #     process_data(piece)

    def append(self, local_path, hdfs_path, buffersize=None):
        """
        Append to a file
        Experimental feature on HDFS
        To enable append on HDFS:
            <property>
                <name>dfs.support.append</name>
                <value>true</value>
            </property>
        """
        params = {'op': 'APPEND'}
        r = requests.post('%s%s' % (self.namenode_url, hdfs_path), params=params)
        print r.headers['location']
        files = {'local_file': open(local_path, 'rb')}
        r = requests.post('%s' % (r.headers['location']), params=params)
        print r.status_code

    def copyFromLocal(self, local_path, hdfs_path, overwrite=None):
        """
        Copy a file from the local filesystem to HDFS

        @param local_path path of the file to copy
        @param hdfs_path hdfs destination path
        @param overwrite true to overwrite already existing files
        @return true on success, raise exception on error
        """
        params = {'op': 'CREATE', 'overwrite': overwrite}
        r = requests.put('%s%s' % (self.namenode_url, hdfs_path), params=params)
        url_location = r.headers['location']
        files = {'local_file': open(local_path, 'rb')}
        r = requests.put(url_location, files=files)


if __name__ == "__main__":
    webHDFS = pyWebHDFS("localhost", 50070, "fabio")