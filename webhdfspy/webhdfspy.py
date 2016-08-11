#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A wrapper library to access Hadoop HTTP REST API"""
__author__ = 'fabio@fabiosouto.me'

import os
import logging
import json

import requests


CONTEXT_ROOT = '/webhdfs/v1'
OFFSET = 32768  # Default offset in bytes


class WebHDFSException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class WebHDFSClient(object):

    def __init__(self, host, port, username=None, logger=None):
        """
        Create a new WebHDFS client.

        When security is on, we need to specify an username
        :param host: hostname of the HDFS namenode
        :param port: port of the namenode
        :param username: used for authentication
        """
        self.host = host
        self.port = port
        self.username = username
        self.namenode_url = 'http://%s:%s%s' % (host, port, CONTEXT_ROOT)
        self.logger = logger or logging.getLogger(__name__)

    def _make_request(self, method, path, params, allow_redirects=False):
        """
        Make an HTTP request to the namenode
        """
        params['user.name'] = self.username
        return requests.request(method,
                                "%s%s" % (self.namenode_url, path),
                                params=params,
                                allow_redirects=allow_redirects)

    def _query(self, method, path, params,
               json_path=['boolean'], allow_redirects=False):
        """
        Call the function to make the request and handle the response
        """
        params['user.name'] = self.username
        r = self._make_request(method, path, params, allow_redirects)
        r.raise_for_status()

        if r.status_code == 200:
            # Some operations return a json while others return
            # a zero content length response.
            if json_path:
                response = json.loads(r.text)
                for key in json_path:
                    response = response[key]
                return response
            return True
        raise WebHDFSException("There was an error with your query")

    def listdir(self, path='/'):
        """
        List all the contents of a directory

        :param path: path of the directory
        :returns: a list of fileStatusProperties, False on error
        """
        self.logger.info("Listing %s", path)
        params = {'op': 'LISTSTATUS'}
        return self._query(method='get', path=path, params=params,
                           json_path=['FileStatuses', 'FileStatus'])

    def mkdir(self, path, permission=None):
        """
        Create a directory hierarchy, like the unix command mkdir -p

        :param path: the path of the directory
        :param permission: dir permissions in octal (0-777)
        """
        self.logger.info("Creating directory %s", path)
        params = {
            'op': 'MKDIRS',
            'permission': permission
        }
        return self._query(method='put', path=path, params=params)

    def remove(self, path, recursive=False):
        """
        Delete a file o directory

        :param path: path of the file or dir to delete
        :param recursive: set to true to delete the content in subdirectories
        """
        self.logger.info("Deleting %s", path)
        params = {
            'op': 'DELETE',
            'recursive': recursive
        }
        return self._query(method='delete', path=path, params=params)

    def rename(self, src, dst):
        """
        Rename a file or directory

        :param src: path of the file or dir to rename
        :param dst: path of the final file/dir
        """
        self.logger.info("Renaming %s", src)
        params = {
            'op': 'RENAME',
            'destination': dst
        }
        return self._query(method='put', path=src, params=params)

    def environ_home(self):
        """
        :returns: the home directory of the user
        """
        self.logger.info("Getting environment home")
        params = {'op': 'GETHOMEDIRECTORY'}
        return self._query(method='get', path='/',
                           params=params, json_path=['Path'])

    def open(self, path, offset=None, length=None, buffersize=None):
        """
        Open a file to read

        :param path: path of the file
        :param offset: starting bit position
        :param length: number of bits to read
        :param buffersize: the size of the buffer used to transfer the data

        :returns: the file data
        """
        self.logger.info("Opening %s", path)
        params = {
            'op': 'OPEN',
            'offset': offset,
            'length': length,
            'buffersize': buffersize
        }
        r = self._make_request(method='get', path=path,
                               params=params, allow_redirects=True)
        return r.text

    def status(self, path):
        """
        Returns the status of a file/dir

        :param path: path of the file/dir
        :returns: a FileStatus dictionary on success, false otherwise
        """
        self.logger.info("Getting status of %s", path)
        params = {'op': 'GETFILESTATUS'}
        return self._query(method='get', path=path, params=params,
                           json_path=['FileStatus'], allow_redirects=True)

    def chmod(self, path, permission):
        """
        Set the permissions of a file or directory

        :param path: path of the file/dir
        :param permission: dir permissions in octal (0-777)
        """
        self.logger.info("Setting permissions of %s to %s", path, permission)
        params = {
            'op': 'SETPERMISSION',
            'permission': permission
        }
        return self._query(method='put', path=path, json_path=[],
                           params=params)

    def create(self, path, file_data, overwrite=None):
        """
        Create a new file in HDFS with the content of file_data

        https://hadoop.apache.org/docs/r1.0.4/webhdfs.html#CREATE

        :param path: the file path to create the file
        :param data: the data to write to the
        """
        self.logger.info("Creating %s", path)
        params = {
            'op': 'CREATE',
            'overwrite': overwrite
        }
        r = self._make_request(method='put', path=path,
                               params=params, allow_redirects=False)
        datanode_url = r.headers['location']

        r = requests.put(datanode_url, data=file_data,
                         headers={'content-type': 'application/octet-stream'})
        r.raise_for_status()
        return True

    def copyfromlocal(self, local_path, hdfs_path, overwrite=None):
        """
        Copy a file from the local filesystem to HDFS

        :param local_path: path of the file to move
        :param hdfs_path: hdfs path to copy the file
        """
        self.logger.info("Copying local file %s to %s", local_path, hdfs_path)
        if not os.path.exists(local_path):
            raise WebHDFSException("The local file {} doesn't exist"
                                   .format(local_path))
        with open(local_path, 'rb') as reader:
            self.create(hdfs_path, reader, overwrite=overwrite)

    def append(self, path, file_data, buffersize=None):
        """
        Append file_data to a file

        :param path: path of the file
        :param file_data: data to append to the file
        :param buffersize: size of the buffer used to transfer the data
        """
        self.logger.info("Appending to file %s", path)
        params = {'op': 'APPEND'}
        r = self._make_request(method='post', path=path, params=params)
        datanode_url = r.headers['location']

        r = requests.post(datanode_url, data=file_data, params=params)
        r.raise_for_status()
        return True

    def set_replication(self, path, replication_factor):
        """
        Set the replication factor of a file

        :param path: path of the file
        :param replication_factor: number of replications, should be > 0
        """
        self.logger.info("Setting replication factor of %s to %s",
                         path, replication_factor)
        params = {
            'op': 'SETREPLICATION',
            'replication': replication_factor
        }
        return self._query(method='put', path=path, params=params)

    def get_checksum(self, path):
        """
        Returns the checksum of a file

        :param path: path of the file
        :returns: FileChecksum JSON
        """
        self.logger.info("Getting checksum of %s", path)
        params = {
            'op': 'GETFILECHECKSUM'
        }
        r = self._make_request(method='get', path=path, params=params)
        r.raise_for_status()
        datanode_url = r.headers['location']

        r = requests.get(datanode_url)
        r.raise_for_status()
        return json.loads(r.text)['FileChecksum']
