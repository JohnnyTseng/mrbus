#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = [
    'get_taipei_route_index', 'get_new_taipei_route_index',
    'TaipeiRoute', 'NewTaipeiRoute'
]

import re
import json
import requests
from time import time
from urlparse import urlparse, parse_qs
from lxml import html

_HEADERS = {
    'Referer': 'https://www.google.com/',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}

def _fetch_text(url, referer=None, encoding=None):

    headers = _HEADERS
    if referer is not None:
        headers = _HEADERS.copy()
        headers['Referer'] = referer

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    if encoding is not None:
        resp.encoding = encoding

    return resp.text

class _RouteIndex(object):

    def __init__(self):
        self._name_rid_map = None

    def _fetch_index_text(self):
        raise NotImplementedError('_fetch_index_text')

    def _parse_to_name_rid_map(self, text):
        raise NotImplementedError('_parse_to_name_rid_map')

    def get_name_rid_map(self):
        if self._name_rid_map is None:
            self._name_rid_map = self._parse_to_name_rid_map(
                self._fetch_index_text()
            )
        return self._name_rid_map

class _TaipeiRouteIndex(_RouteIndex):

    URL = 'http://e-bus.taipei.gov.tw/'

    def _fetch_index_text(self):
        return _fetch_text(
            self.URL,
            encoding = 'utf-8'
        )

    JS_BLOCK_COMMENT_RE = re.compile(ur'/\*.*?\*/', re.S)
    EBUS_CALL_RE = re.compile(ur'eBus1?(?:_0)?\(".*?","(?P<rid>.+?)","(?P<name>.+?)"\)')
    EBUS_A_RE = re.compile(ur'''<a href='javascript:openEbus1?\("(?P<rid>.+?)"\)'>(?P<name>.+?)</a>''')

    def _parse_to_name_rid_map(self, text):

        name_rid_map = {}

        nocomment_text = self.JS_BLOCK_COMMENT_RE.sub('', text)
        for m in self.EBUS_CALL_RE.finditer(nocomment_text):
            name_rid_map[m.group('name')] = m.group('rid')
        for m in self.EBUS_A_RE.finditer(nocomment_text):
            name_rid_map[m.group('name')] = m.group('rid')

        return name_rid_map

class _NewTaipeiRouteIndex(_RouteIndex):

    URL = 'http://e-bus.ntpc.gov.tw/'

    def _fetch_index_text(self):
        return _fetch_text(self.URL, encoding='utf-8')

    def _parse_to_name_rid_map(self, text):

        name_rid_map = {}

        root = html.fromstring(text)
        for a in root.xpath('//a'):

            r = urlparse(a.get('href'))
            if r.path == '../NTPCRoute/Tw/Map':

                d = parse_qs(r.query)
                if 'rid' in d:
                    name_rid_map[a.text] = d['rid'][0]

        return name_rid_map

_tpri = None

def get_taipei_route_index():

    global _tpri

    if _tpri is None:
        _tpri = _TaipeiRouteIndex()

    return _tpri

_ntri = None

def get_new_taipei_route_index():

    global _ntri

    if _ntri is None:
        _ntri = _NewTaipeiRouteIndex()

    return _ntri

class _Route(object):

    # NOTE: It's an abstract class, please inherit and override those attrs:
    #
    # 1. PAGE_URL_TPL
    # 2. API_URL_TPL
    #

    # sec = 0 # 去程
    # sec = 1 # 回程
    _PAGE_URL_TPL = ''

    @classmethod
    def _format_page_url(cls, rid, sec):
        return cls._PAGE_URL_TPL.format(rid=rid, sec=sec)

    _API_URL_TPL = ''

    @classmethod
    def _format_api_url(cls, rid, sec):
        return cls._API_URL_TPL.format(rid=rid, sec=sec, _=int(time()*1000))

    def __init__(self, rid, sec):

        self._rid = rid
        self._sec = sec

        self._idx_name_map = None
        self._idx_eta_map = None
        self._idx_bus_map = None

    def _fetch_page_text(self):
        return _fetch_text(
            self._format_page_url(self._rid, self._sec),
            referer = _TaipeiRouteIndex.URL
        )

    def _fetch_api_text(self):
        return _fetch_text(
            self._format_api_url(self._rid, self._sec),
            referer = self._format_page_url(self._rid, self._sec),
        )

    def _parse_to_idx_name_map(self, page_text):

        idx_name_map = {}

        root = html.fromstring(page_text)
        for stop_div in root.xpath("//*[contains(@class, 'stop ')]"):
            stop_idx = int(
                stop_div
                .xpath(".//*[@class='eta']")[0]
                .get('id')
                .partition('_')[2]
            )
            stop_name = stop_div.xpath(".//*[@class='stopName']")[0][0].text
            idx_name_map[stop_idx] = stop_name

        return idx_name_map

    def _transform_to_idx_eta_map(self, d):

        # eta -> 255 means 未發車
        # eta -> 254 means 末班車已過

        return {
            d['idx']: d['eta']
            for d in d['Etas']
        }

    def _transform_to_idx_bus_map(self, d):

        # TODO: what is fl and io?

        return {
            d['idx']: d
            for d in d['Buses']
        }

    def _parse_api_text(self, api_text):
        d = json.loads(api_text)
        return (
            self._transform_to_idx_eta_map(d),
            self._transform_to_idx_bus_map(d)
        )

    def get_idx_name_map(self):
        if self._idx_name_map is None:
            self._idx_name_map = self._parse_to_idx_name_map(
                self._fetch_page_text()
            )
        return self._idx_name_map

    def get_idx_eta_map(self):
        if self._idx_eta_map is None:
            self._idx_eta_map, self._idx_bus_map = self._parse_api_text(
                self._fetch_api_text()
            )
        return self._idx_eta_map

    def get_idx_bus_map(self):
        if self._idx_bus_map is None:
            self._idx_eta_map, self._idx_bus_map = self._parse_api_text(
                self._fetch_api_text()
            )
        return self._idx_bus_map

class TaipeiRoute(_Route):

    _PAGE_URL_TPL = 'http://e-bus.taipei.gov.tw/newmap/Tw/Map?rid={rid}&sec={sec}'
    _API_URL_TPL = 'http://e-bus.taipei.gov.tw/newmap/Js/RouteInfo?rid={rid}&sec={sec}&_={_}'

class NewTaipeiRoute(_Route):

    _PAGE_URL_TPL = 'http://e-bus.ntpc.gov.tw/NTPCRoute/Tw/Map?rid={rid}&sec={sec}'
    _API_URL_TPL = 'http://e-bus.ntpc.gov.tw/NTPCRoute/Js/RouteInfo?rid={rid}&sec={sec}&_={_}'

if __name__ == '__main__':

    import uniout
    from pprint import pprint

    tpri = get_taipei_route_index()
    pprint(tpri.get_name_rid_map())

    npri = get_new_taipei_route_index()
    pprint(npri.get_name_rid_map())

    tpr1 = TaipeiRoute('10723', 0)
    pprint(tpr1.get_idx_name_map())
    pprint(tpr1.get_idx_eta_map())
    pprint(tpr1.get_idx_bus_map())

    ntr1 = NewTaipeiRoute('114', 0)
    pprint(ntr1.get_idx_name_map())
    pprint(ntr1.get_idx_eta_map())
    pprint(ntr1.get_idx_bus_map())
