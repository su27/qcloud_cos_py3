Qcloud COS SDK for Python 3
===============================

version: 0.1.4

Overview
--------

A 3rd-party SDK for Qcloud COS and Python 3

Installation / Usage
--------------------

Docs: https://su27.github.io/qcloud_cos_py3/

Use pip to install:

    $ pip install qcloud_cos_py3

Or clone the repo:

    $ git clone https://github.com/su27/qcloud_cos_py3.git
    $ python setup.py install

Contributing
------------

    $ virtualenv -p python3.6 venv
    $ source venv/bin/activate
    $ pip install -r requirements.txt
    $ cp tests/config_example.py tests/config.py

    # Fill the blanks in the file and run
    $ make test-coverage

It's originally forked from [cos-python3-sdk](https://github.com/imu-hupeng/cos-python3-sdk)

Example
-------

Please check `tests/test_cos.py`
