========
Overview
========

A set of glue code and utilities to make using elemeno AI platform a smooth experience

* Free software: Apache Software License 2.0

Installation
============

::

    pip install elemeno-ai-sdk

You can also install the in-development version with::

    pip install https://gitlab.com/elemeno-ai/python-elemeno-ai-sdk/-/archive/master/python-elemeno-ai-sdk-master.zip


Documentation
=============


elemeno-ai-sdk.readthedocs.io


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox

Versioning
==========

Bumping version is as simple as running `python -m bumpversion --new-version 0.0.5 patch`
