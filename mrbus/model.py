#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import time, sleep
from mosql.query import select, update
from mrbus.util import debug, get_now_dt
from mrbus.pool import Pool
from mrbus.gov import *
from mrbus.conn import db

_pool = Pool()

def merge_routes_on_route_indexes():

    start_ts = time()

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

    debug('Took {:.3f}s on networking.'.format(time()-start_ts))

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
            where   = {'id not in': rid_rd_map},
            columns = ('id', ),
            for_    = 'update'
        ))

        to_mark_on_index_false_rids = tuple(rid for rid, in cur)
        debug('len(to_mark_on_index_false_rids) = {!r}'.format(
            len(to_mark_on_index_false_rids)
        ))

        cur.execute(update(
            'route',
            where = {'id': to_mark_on_index_false_rids},
            set   = {'on_index': False}
        ))

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

    debug('Took {:.3f}s.'.format(time()-start_ts))

def _create_route_page_pair(route_id):

    # rc : region code
    # rid: the route page's rid
    rc, _, rid = route_id.partition('_')

    route_page_class = None

    if rc == 'tp':
        route_page_class = TaipeiRoutePage
    elif rc == 'nt':
        route_page_class = NewTaipeiRoutePage

    if route_page_class is None:
        raise RouteIDError('bad region code: {!r}'.format(rc))

    return (
        route_page_class(rid, 0),
        route_page_class(rid, 1)
    )

def _query_route_ids_it(chunk_size=100):

    offset = 0

    while True:

        with db as cur:

            cur.execute('''
                select
                    id
                from
                    route
                where
                    on_index = true
                order by
                    created_ts,
                    id
                limit
                    %s
                offset
                    %s
            ''', (chunk_size, offset))

            route_ids = [route_id for route_id, in cur]
            if not route_ids:
                break

            yield route_ids

        offset += chunk_size

if __name__ == '__main__':

    import uniout
    from pprint import pprint

    for rids in _query_route_ids_it():
        print len(rids)

    import sys; sys.exit()

    rids_it = _query_route_ids_it(3)
    print next(rids_it)
    print next(rids_it)

    import sys; sys.exit()

    merge_routes_on_route_indexes()

    import sys; sys.exit()

    rp0, rp1 = _create_route_page_pair('tp_10723')
    pprint(rp0.get_idx_eta_map())
