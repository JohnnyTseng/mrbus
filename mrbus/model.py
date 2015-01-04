#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mrbus.gov import *
from mrbus.exc import *
from mrbus.pool import Pool, do
from mrbus.util import db

def get_route_id(route_name):

    # NOTE: the route id here is different from rid in gov

    tpri = get_taipei_route_index()
    tp_name_rid_map = tpri.get_name_rid_map()
    if route_name in tp_name_rid_map:
        return 'tp_{}'.format(tp_name_rid_map[route_name])

    ntri = get_new_taipei_route_index()
    nt_name_rid_map = ntri.get_name_rid_map()
    if route_name in nt_name_rid_map:
        return 'nt_{}'.format(nt_name_rid_map[route_name])

    return None

def _get_route_page_pair(route_id):

    prefix, _, rid = route_id.partition('_')

    if prefix == 'tp':
        return (
            TaipeiRoutePage(rid, 0),
            TaipeiRoutePage(rid, 1)
        )

    if prefix == 'nt':
        return (
            NewTaipeiRoutePage(rid, 0),
            NewTaipeiRoutePage(rid, 1),
        )

    return None

_pool = Pool()

class Route(dict):

    @classmethod
    def init_by_name(self, name):

        route_id = get_route_id(name)
        if route_id is None:
            raise RouteNameError(name)

        route = Route(route_id)
        route['name'] = name

        return route

    def __init__(self, id):

        self['id'] = id

        route_page_pair = _get_route_page_pair(id)
        if route_page_pair is None:
            raise RouteIDError(id)

        self._route_page_pair = route_page_pair

    def fetch_stop_waitings(self):

        for rpage in self._route_page_pair:
            rpage.clear_cache()

        # --- speed up ---
        # use threads to fetch data

        tasks = []
        for rpage in self._route_page_pair:
            tasks.append(rpage.get_idx_name_map)
            tasks.append(rpage.get_idx_eta_map)
        _pool.map(do, tasks)

        # --- end ---

        stop_waitings = []

        it_is_return = False
        for rpage in self._route_page_pair:

            idx_name_map = rpage.get_idx_name_map()
            idx_eta_map = rpage.get_idx_eta_map()

            idxs = idx_name_map.keys()
            idxs.sort()

            for idx in idxs:

                eta = idx_eta_map[idx]

                stop_waitings.append({
                    'name'        : idx_name_map[idx],
                    'it_is_return': it_is_return,
                    'waiting_min' : eta if eta not in (254, 255) else float('nan'),
                    'message'     : ETA_MESSAGE_MAP.get(eta, '')
                })

            it_is_return = not it_is_return

        return stop_waitings

if __name__ == '__main__':

    import uniout
    from pprint import pprint

    route = Route.init_by_name('1')
    pprint(route.fetch_stop_waitings())
    print

    route = Route.init_by_name('241')
    pprint(route.fetch_stop_waitings())
