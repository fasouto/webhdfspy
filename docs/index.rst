.. webhdfspy documentation master file, created by
   sphinx-quickstart on Thu Apr 23 18:39:29 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=========
webhdfspy
=========

A Python 2/3 wrapper library to access `Hadoop WebHDFS REST API <https://hadoop.apache.org/docs/r1.0.4/webhdfs.html>`_


Installation
============

To install webhdfspy from PyPI::

    $ pip install webhdfspy


Python versions
===============

webhdfspy supports Python 2.7 and 3.4


Usage
=====
::
    
    >>> import webhdfspy
    >>> webHDFS = webhdfspy.WebHDFSClient("localhost", 50070, "username")
    >>> print(webHDFS.listdir('/'))
    []
    >>> webHDFS.mkdir('/foo')
    True
    >>> print(webHDFS.listdir('/'))
    [{u'group': u'supergroup', u'permission': u'755', u'blockSize': 0, u'accessTime': 0, u'pathSuffix': u'foo', u'modificationTime': 1429805040695, u'replication': 0, u'length': 0, u'childrenNum': 0, u'owner': u'username', u'storagePolicy': 0, u'type': u'DIRECTORY', u'fileId': 16387}]
    >>> print webHDFS.create('/foo/foo.txt', "just put some text here", True)
    True
    >>> print webHDFS.open('/pywebhdfs_test/foo.txt') 
    just put some text here
    >>> webHDFS.remove('/foo')
    True
    >>> print(webHDFS.listdir('/'))
    []


API Documentation
=================

.. autoclass:: webhdfspy.webhdfspy.WebHDFSClient
	:members:  __init__, listdir, mkdir, remove, rename, environ_home, open, status, chmod, create, append, set_replication

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

