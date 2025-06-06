Welcome to Qcloud COS SDK for Python 3's documentation!
=======================================================

.. toctree::
   :maxdepth: 2

.. automodule:: qcloud_cos_py3.cos

.. autoclass:: CosBucket
    :members:

The ``CosBucket`` constructor accepts an optional ``endpoint`` argument which
defaults to ``"{region}.file.myqcloud.com"``. Providing a custom endpoint makes
it possible to use official domains and enables HTTPS access.

``async_upload_file`` relies on :mod:`asyncio` to execute ``upload_file`` in a
background thread, so it works without extra dependencies.



Indices and tables
==================
* :ref:`search`
