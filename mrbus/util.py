#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import inspect
from datetime import datetime

def debug(s):
    print >> sys.stderr, \
        'debug:', \
        '{}:'.format(inspect.currentframe().f_back.f_code.co_name), \
        s

def get_now_dt():
    return datetime.now()

def escape_like_operand(s):
    return s.replace('\\', '\\\\').replce('_', '\_').replace('%', '\%')
