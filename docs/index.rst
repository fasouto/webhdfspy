=========
webhdfspy
=========

A Python wrapper library to access `Hadoop WebHDFS REST API <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`_


Installation
============

To install webhdfspy from PyPI::

    $ pip install webhdfspy


Python versions
===============

webhdfspy requires Python 3.9+


Usage
=====
::

    >>> import webhdfspy
    >>> client = webhdfspy.WebHDFSClient("localhost", 50070, "username")
    >>> print(client.listdir('/'))
    []
    >>> client.mkdir('/foo')
    True
    >>> print(client.listdir('/'))
    [{'group': 'supergroup', 'permission': '755', ...}]
    >>> client.create('/foo/foo.txt', "just put some text here", overwrite=True)
    True
    >>> print(client.open('/foo/foo.txt'))
    just put some text here
    >>> client.remove('/foo')
    True

Using a context manager::

    >>> with webhdfspy.WebHDFSClient("localhost", 50070, "username") as client:
    ...     client.listdir('/')
    []

HTTPS support::

    >>> client = webhdfspy.WebHDFSClient("host", 9871, "user", scheme="https")


API Documentation
=================

.. autoclass:: webhdfspy.WebHDFSClient
	:members:

Exceptions
----------

.. autoclass:: webhdfspy.WebHDFSException
	:members:

.. autoclass:: webhdfspy.WebHDFSRemoteException
	:members:

.. autoclass:: webhdfspy.WebHDFSConnectionError
	:members:


WebHDFS documentation
=====================

https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
