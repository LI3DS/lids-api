LI3DS API
=========

|unix_build| |license|

Api for accessing metadata of a li3ds datastore.

* Create your virtualenv
* Install dependencies (dev)::

    pip install -e .[dev,doc]

* Install dependencies (prod)::

    pip install .[prod]

* (dev) Duplicate the conf/api_li3ds.sample.yml file to conf/api_li3ds.yml and adapt parameters

* (dev) Launch the application using::

    python api_li3ds/wsgi.py

* (dev) Go to https://localhost:5000 and start to play with the API

.. image:: https://raw.githubusercontent.com/LI3DS/api-li3ds/master/screen-api.png
    :align: center


.. |unix_build| image:: https://img.shields.io/travis/LI3DS/api-li3ds/master.svg?style=flat-square&label=unix%20build
    :target: http://travis-ci.org/LI3DS/api-li3ds
    :alt: Build status of the master branch

.. |license| image:: https://img.shields.io/badge/license-GPLv3-blue.svg?style=flat-square
    :target: https://raw.githubusercontent.com/LI3DS/api-li3ds/master/LICENSE
    :alt: Package license
