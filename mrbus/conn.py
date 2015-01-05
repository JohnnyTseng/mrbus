#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
from getpass import getuser
from mosql.db import Database

# let psycopg2 return unicode instead of 8-bit string
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

db = Database(psycopg2, user=getuser())

