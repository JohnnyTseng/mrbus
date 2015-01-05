#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime

def get_now_dt():
    return datetime.now()

def escape_like_operand(s):
    return s.replace('\\', '\\\\').replce('_', '\_').replace('%', '\%')
