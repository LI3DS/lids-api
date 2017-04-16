#!/bin/bash

/etc/init.d/postgresql start
python3 api_li3ds/wsgi.py
