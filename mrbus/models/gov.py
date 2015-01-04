#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import requests
from time import time
from lxml import html

session = requests.Session()
session.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'

def _session_get_text(url, referer=None):

    global session

    if referer is None:
        referer = 'https://www.google.com/'

    session.headers['Referer'] = referer

    resp = session.get(url)
    resp.raise_for_status()

    last_url = url

    return resp.text

class TaipeiIndex(object):

    URL = 'http://e-bus.taipei.gov.tw/'

class TaipeiRoute(object):

    # sec = 0 # 去程
    # sec = 1 # 回程
    PAGE_URL_TPL = 'http://e-bus.taipei.gov.tw/newmap/Tw/Map?rid={rid}&sec={sec}'

    @classmethod
    def _format_page_url(cls, rid, sec):
        return cls.PAGE_URL_TPL.format(rid=rid, sec=sec)

    API_URL_TPL = 'http://e-bus.taipei.gov.tw/newmap/Js/RouteInfo?rid={rid}&sec={sec}&_={_}'

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

if __name__ == '__main__':

    import uniout
    from pprint import pprint

    tpr1 = TaipeiRoute('10723')
    pprint(tpr1.get_idx_name_map(0))
    pprint(tpr1.get_idx_eta_map(0))
