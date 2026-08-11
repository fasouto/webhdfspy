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


Binary and large files
======================

``open()`` returns text. Use ``read()`` for binary data, and ``stream()`` or
``copytolocal()`` for files too large to hold in memory::

    >>> client.read('/data/image.png')
    b'\x89PNG\r\n...'
    >>> with client.stream('/data/huge.parquet') as chunks:
    ...     for chunk in chunks:
    ...         process(chunk)
    >>> client.copytolocal('/data/huge.parquet', 'local.parquet')
    True


High availability
=================

Pass every namenode and the client fails over when the active one is standby
or unreachable::

    >>> client = webhdfspy.WebHDFSClient(["nn1", "nn2"], 9870)


Secured clusters
================

``auth`` accepts any ``requests`` auth handler, and ``verify``/``cert``
control TLS::

    >>> from requests_kerberos import HTTPKerberosAuth
    >>> client = webhdfspy.WebHDFSClient(
    ...     "host", 9871, scheme="https",
    ...     auth=HTTPKerberosAuth(),
    ...     verify="/etc/pki/ca-trust/ca-bundle.crt",
    ... )

Delegation tokens authenticate subsequent requests::

    >>> token = client.get_delegation_token("renewer")
    >>> client.set_delegation_token(token["urlString"])


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
