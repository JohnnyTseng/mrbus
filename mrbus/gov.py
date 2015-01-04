#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = ['TaipeiIndex', 'NewTaipeiIndex', 'TaipeiRoute', 'NewTaipeiRoute']

import re
import json
import requests
from time import time
from urlparse import urlparse, parse_qs
from lxml import html

session = requests.Session()
session.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'

def _session_get_text(url, referer=None, encoding=None):

    global session

    if referer is None:
        referer = 'https://www.google.com/'

    session.headers['Referer'] = referer

    resp = session.get(url)
    resp.raise_for_status()

    last_url = url

    if encoding is not None:
        resp.encoding = encoding

    return resp.text

class TaipeiIndex(object):

    URL = 'http://e-bus.taipei.gov.tw/'

    def _get_index_text(self):
        return _session_get_text(
            self.URL,
            encoding = 'utf-8'
        )

    JS_BLOCK_COMMENT_RE = re.compile(ur'/\*.*?\*/', re.S)
    EBUS_CALL_RE = re.compile(ur'eBus1?(?:_0)?\(".*?","(?P<rid>.+?)","(?P<name>.+?)"\)')
    EBUS_A_RE = re.compile(ur'''<a href='javascript:openEbus1?\("(?P<rid>.+?)"\)'>(?P<name>.+?)</a>''')

    def get_name_rid_map(self):

        name_rid_map = {}

        text = self.JS_BLOCK_COMMENT_RE.sub('', self._get_index_text())
        for m in self.EBUS_CALL_RE.finditer(text):
            name_rid_map[m.group('name')] = m.group('rid')
        for m in self.EBUS_A_RE.finditer(text):
            name_rid_map[m.group('name')] = m.group('rid')

        return name_rid_map

class NewTaipeiIndex(object):

    URL = 'http://e-bus.ntpc.gov.tw/'

    def _get_index_text(self):
        return _session_get_text(self.URL, encoding='utf-8')

    def get_name_rid_map(self):

        name_rid_map = {}

        root = html.fromstring(self._get_index_text())
        for a in root.xpath('//a'):

            r = urlparse(a.get('href'))
            if r.path == '../NTPCRoute/Tw/Map':

                d = parse_qs(r.query)
                if 'rid' in d:
                    name_rid_map[a.text] = d['rid'][0]

        return name_rid_map

class _Route(object):

    # NOTE: It's an abstract class, please inherit and override those attrs:
    #
    # 1. PAGE_URL_TPL
    # 2. API_URL_TPL
    #

    # sec = 0 # 去程
    # sec = 1 # 回程
    PAGE_URL_TPL = ''

    @classmethod
    def _format_page_url(cls, rid, sec):
        return cls.PAGE_URL_TPL.format(rid=rid, sec=sec)

    API_URL_TPL = ''

    @classmethod
    def _format_api_url(cls, rid, sec):
        return cls.API_URL_TPL.format(rid=rid, sec=sec, _=int(time()*1000))

    def __init__(self, rid):
        self.rid = rid

    def _get_page_text(self, sec):
        return _session_get_text(
            self._format_page_url(self.rid, sec),
            referer = TaipeiIndex.URL
        )

    def _get_api_text(self, sec):
        return _session_get_text(
            self._format_api_url(self.rid, sec),
            referer = self._format_page_url(self.rid, sec),
        )

    def get_idx_name_map(self, sec):

        idx_name_map = {}

        root = html.fromstring(self._get_page_text(sec))
        for stop_div in root.xpath("//*[contains(@class, 'stop ')]"):
            stop_idx = int(stop_div.xpath(".//*[@class='eta']")[0].get('id').partition('_')[2])
            stop_name = stop_div.xpath(".//*[@class='stopName']")[0][0].text
            idx_name_map[stop_idx] = stop_name

        return idx_name_map

    def get_idx_eta_map(self, sec):

        # TODO: get the plate number
        #
        # {u'Buses': [{u'bn': u'016-FR', u'fl': u'l', u'idx': 14, u'io': u'o'},
        #             {u'bn': u'035-FR', u'fl': u'l', u'idx': 16, u'io': u'o'},
        #             {u'bn': u'013-FR', u'fl': u'l', u'idx': 4, u'io': u'i'}],

        # eta -> 255 means 未發車

        return {
            d['idx']: d['eta']
            for d in json.loads(self._get_api_text(sec))['Etas']
        }

class TaipeiRoute(_Route):

    PAGE_URL_TPL = 'http://e-bus.taipei.gov.tw/newmap/Tw/Map?rid={rid}&sec={sec}'
    API_URL_TPL = 'http://e-bus.taipei.gov.tw/newmap/Js/RouteInfo?rid={rid}&sec={sec}&_={_}'

class NewTaipeiRoute(_Route):

    PAGE_URL_TPL = 'http://e-bus.ntpc.gov.tw/NTPCRoute/Tw/Map?rid={rid}&sec={sec}'
    API_URL_TPL = 'http://e-bus.ntpc.gov.tw/NTPCRoute/Js/RouteInfo?rid={rid}&sec={sec}&_={_}'

if __name__ == '__main__':

    import uniout
    from pprint import pprint

    tpi = TaipeiIndex()
    pprint(tpi.get_name_rid_map())

    npi = NewTaipeiIndex()
    pprint(npi.get_name_rid_map())

    import sys; sys.exit()

    tpr1 = TaipeiRoute('10723')
    pprint(tpr1.get_idx_name_map(0))
    pprint(tpr1.get_idx_eta_map(0))

    ntr1 = NewTaipeiRoute('114')
    pprint(ntr1.get_idx_name_map(0))
    pprint(ntr1.get_idx_eta_map(0))
