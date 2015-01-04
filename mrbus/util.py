#!/usr/bin/env python
# -*- coding: utf-8 -*-

def escape_like_operand(s):
    return s.replace('\\', '\\\\').replce('_', '\_').replace('%', '\%')
