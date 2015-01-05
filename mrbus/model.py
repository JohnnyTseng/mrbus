#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import sleep
from mosql.query import select
from mrbus.util import debug, get_now_dt
from mrbus.pool import Pool
from mrbus.gov import *
from mrbus.conn import db

_pool = Pool()

def merge_routes_on_route_indexes():

    # set up

    global _pool

    # rc: region code (Taipei -> tp; NewTaipei -> nt)
    rc_ri_pairs = [
        ('tp', TaipeiRouteIndex()),
        ('nt', NewTaipeiRouteIndex())
    ]

    # fetch them asyncly
    # the data will be cached in route index instance

    for _, ri in rc_ri_pairs:
        _pool.apply_async(ri.get_name_rid_map)

    _pool.join()

    # transform

    # rid here is route.id in db, not the route index's rid
    # rd: route dict
    rid_rd_map = {}

    for rc, ri in rc_ri_pairs:
        for rname, ri_rid in ri.get_name_rid_map().iteritems():
            rid = '{}_{}'.format(rc, ri_rid)
            rid_rd_map[rid] = {
                'id'  : rid,
                'name': rname
            }

    # merge into db

    with db as cur:

        cur.execute(select(
            'route',
            where   = {'id': rid_rd_map},
            columns = ('id', 'name'),
            for_    = 'update'
        ))

        to_skip_rid_set = set()
        to_update_rid_set = set()
        for rid, rname in cur:
            if rname == rid_rd_map[rid]['name']:
                to_skip_rid_set.add(rid)
            else:
                to_update_rid_set.add(rid)

        now_dt = get_now_dt()
        to_update_rds = []
        to_insert_rds = []
        for rid in rid_rd_map:
            if rid in to_skip_rid_set:
                continue
            elif rid in to_update_rid_set:
                rd = rid_rd_map[rid]
                rd['updated_ts'] = now_dt
                to_update_rds.append(rd)
            else:
                rd = rid_rd_map[rid]
                rd['updated_ts'] = now_dt
                rd['created_ts'] = now_dt
                to_insert_rds.append(rd)

        debug('len(to_update_rds) = {!r}'.format(len(to_update_rds)))
        debug('len(to_insert_rds) = {!r}'.format(len(to_insert_rds)))

        if to_update_rds:
            cur.executemany('''
                update
                    route
                set
                    name       = %(name)s,
                    updated_ts = %(updated_ts)s
                where
                    id = %(id)s
            ''', to_update_rds)

        if to_insert_rds:
            cur.executemany('''
                insert into
                    route (id, name, updated_ts, created_ts)
                values
                    (%(id)s, %(name)s, %(updated_ts)s, %(created_ts)s)
            ''', to_insert_rds)

if __name__ == '__main__':

    import uniout
    from pprint import pprint

    merge_routes_on_route_indexes()
