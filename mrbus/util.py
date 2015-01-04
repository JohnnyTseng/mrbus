#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
from getpass import getuser
from mosql.db import Database

db = Database(psycopg2, user=getuser())

def escape_like_operand(s):
    return s.replace('\\', '\\\\').replce('_', '\_').replace('%', '\%')
