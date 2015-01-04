#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = ['RouteNameError', 'RouteIDError']

class RouteNameError(KeyError): pass

class RouteIDError(KeyError): pass
