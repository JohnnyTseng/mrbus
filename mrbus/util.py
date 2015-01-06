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

def ensure_unicode(x, encoding='utf-8'):

    if isinstance(x, unicode):
        return x

    if isinstance(x, str):
        s = x
    else:
        s = str(x)

    return s.decode(encoding)

def escape_like_operand(s):
    # TODO: shall use mosql's once mosql has it
    return ensure_unicode(s).replace('\\', '\\\\').replace('_', '\_').replace('%', '\%')
