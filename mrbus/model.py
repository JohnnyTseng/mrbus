#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import time
from mosql.db import all_to_dicts
from mrbus.util import debug, get_now_dt, escape_like_operand
from mrbus.pool import Pool
from mrbus.gov import *
from mrbus.conn import db

_pool = Pool()

def sync_routes_on_all_route_indexes():

    start_ts = time()

    # set up

    # rc: region code (Taipei -> tp; NewTaipei -> nt)
    # ri: route index
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

        cur.execute('''
            select
                id
            from
                route
            where
                id not in %s
            for update
        ''', (tuple(rid_rd_map), ))

        to_mark_on_index_false_rids = tuple(rid for rid, in cur)
        debug('len(to_mark_on_index_false_rids) = {!r}'.format(
            len(to_mark_on_index_false_rids)
        ))

        if to_mark_on_index_false_rids:
            cur.execute('''
                update
                    route
                set
                    on_index   = false,
                    updated_ts = %s
                where
                    id in %s
            ''', (get_now_dt(), to_mark_on_index_false_rids))

    with db as cur:

        cur.execute('''
            select
                id, name
            from
                route
            where
                id in %s
            for update
        ''', (tuple(rid_rd_map), ))

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
    # it: iterator

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

# nzscode: nonzero status code
_ETA_NZSCODE_MAP = {
    255: 1,
    254: 2,
    253: 3,
    252: 4,
}

# smessage: status_message
_NZSCODE_SMESSAGE_MAP = {
    1: u'未發車',
    2: u'已過末班',
    3: u'交管不停靠',
    4: u'今日未營運',
}

def _sync_stops_n_phis_on_route_page_pair(route_id, route_page_pair):

    # merge stops first

    sname_set = set()
    for rpage in route_page_pair:
        sname_set.update(rpage.get_idx_name_map().itervalues())

    with db as cur:
        cur.execute('''
            select
                name, id
            from
                stop
            where
                name in %s
        ''', (tuple(sname_set), ))
        sname_sid_map = dict(cur)

    now_dt = get_now_dt()
    # sds: stop dicts
    to_insert_sds = []
    for sname in sname_set:
        if sname not in sname_sid_map:
            to_insert_sds.append({
                'name'     : sname,
                'created_ts': now_dt
            })

    debug('len(to_insert_sds) = {!r}'.format(len(to_insert_sds)))

    if to_insert_sds:

        with db as cur:

            cur.executemany('''
                insert into
                    stop (name, created_ts)
                values
                    (%(name)s, %(created_ts)s)
            ''', to_insert_sds)

            cur.execute('''
                select
                    name, id
                from
                    stop
                where
                    name in %s
            ''', (tuple(sd['name'] for sd in to_insert_sds), ))

            sname_sid_map.update(cur)

    # then merge phi

    # pk: phi primary key: (route_id, stop_id, serial_no)
    pks = []
    # pd: phi dict
    pk_pd_map = {}

    serial_no = 0
    it_is_return = False
    last_waiting_min = None
    for rpage in route_page_pair:

        idx_sname_map = rpage.get_idx_name_map()
        idx_eta_map = rpage.get_idx_eta_map()

        idxs = idx_sname_map.keys()
        idxs.sort()

        for idx in idxs:

            eta = idx_eta_map[idx]

            stop_id = sname_sid_map[idx_sname_map[idx]]
            if eta in _ETA_NZSCODE_MAP:
                status_code = _ETA_NZSCODE_MAP[eta]
                waiting_min = None
            else:
                status_code = 0
                waiting_min = eta

            # interval_min is the time from last stop to this stop
            # so the driving_min will be sum(interval_min[here+1:there+1])
            if (
                last_waiting_min is not None and
                waiting_min is not None and
                waiting_min >= last_waiting_min
            ):
                interval_min = waiting_min-last_waiting_min
            else:
                interval_min = None

            pk = (route_id, serial_no)

            pks.append(pk)
            pk_pd_map[pk] = {
                'route_id'    : route_id,
                'serial_no'   : serial_no,
                'it_is_return': it_is_return,
                'stop_id'     : stop_id,
                'status_code' : status_code,
                'waiting_min' : waiting_min,
                'interval_min': interval_min
            }

            serial_no += 1
            last_waiting_min = waiting_min

        it_is_return = not it_is_return

    with db as cur:

        cur.execute('''
            select
                route_id, serial_no
            from
                phi
            where
                (route_id, serial_no) in %s
            for update
        ''', (tuple(pks), ))
        existent_pk_set = set(cur)

        now_dt = get_now_dt()
        to_update_pds = []
        to_insert_pds = []
        for pk in pks:
            if pk in existent_pk_set:
                pd = pk_pd_map[pk]
                pd['updated_ts'] = now_dt
                to_update_pds.append(pd)
            else:
                pd = pk_pd_map[pk]
                pd['updated_ts'] = now_dt
                pd['created_ts'] = now_dt
                to_insert_pds.append(pd)

        debug('len(to_update_pds) = {!r}'.format(len(to_update_pds)))
        debug('len(to_insert_pds) = {!r}'.format(len(to_insert_pds)))

        if to_update_pds:
            cur.executemany('''
                update
                    phi
                set
                    it_is_return = %(it_is_return)s,
                    stop_id      = %(stop_id)s,
                    status_code  = %(status_code)s,
                    waiting_min  = %(waiting_min)s,
                    interval_min = coalesce(
                        (interval_min+%(interval_min)s)/2,
                        %(interval_min)s,
                        interval_min
                    ),
                    updated_ts   = %(updated_ts)s
                where
                    (route_id, serial_no) = (%(route_id)s, %(serial_no)s)
            ''', to_update_pds)

        if to_insert_pds:
            cur.executemany('''
                insert into
                    phi (
                        route_id,
                        serial_no,
                        it_is_return,
                        stop_id,
                        status_code,
                        waiting_min,
                        interval_min,
                        updated_ts,
                        created_ts
                    )
                values (
                    %(route_id)s,
                    %(serial_no)s,
                    %(it_is_return)s,
                    %(stop_id)s,
                    %(status_code)s,
                    %(waiting_min)s,
                    %(interval_min)s,
                    %(updated_ts)s,
                    %(created_ts)s
                )
            ''', to_insert_pds)

def sync_stops_n_phis_of_all_routes():

    start_ts = time()
    networking_sec = 0

    for rids in _query_route_ids_it():

        networing_start_ts = time()

        # rpagep: route page pair
        rid_rpagep_map = {}

        for rid in rids:

            rpagep = _create_route_page_pair(rid)
            rid_rpagep_map[rid] = rpagep

            # fetch pages asyncly
            for rpage in rpagep:
                _pool.apply_async(rpage.get_idx_name_map)
                _pool.apply_async(rpage.get_idx_eta_map)

        # wait for fetching pages
        _pool.join()

        networking_sec += time()-networing_start_ts

        # merge this route's stops
        for rid in rids:
            _sync_stops_n_phis_on_route_page_pair(rid, rid_rpagep_map[rid])

    debug('Took {:.3f}s on networking.'.format(networking_sec))
    debug('Took {:.3f}s.'.format(time()-start_ts))
    # debug: sync_stops_n_phis_of_all_routes: Took 110.586s on networking.
    # debug: sync_stops_n_phis_of_all_routes: Took 133.775s.

def sync_stops_n_phis_of_route(route_id):

    start_ts = time()

    rpagep = _create_route_page_pair(route_id)

    # fetch pages asyncly
    for rpage in rpagep:
        _pool.apply_async(rpage.get_idx_name_map)
        _pool.apply_async(rpage.get_idx_eta_map)

    # wait for fetching pages
    _pool.join()

    debug('Took {:.3f}s on networking.'.format(time()-start_ts))

    _sync_stops_n_phis_on_route_page_pair(route_id, rpagep)

    debug('Took {:.3f}s.'.format(time()-start_ts))

def query_stops(keyword):

    with db as cur:

        cur.execute('''
            select
                id,
                name
            from
                stop
            where
                name like %s
            order by
                char_length(name)
        ''', (u'%{}%'.format(escape_like_operand(keyword)), ))

        return all_to_dicts(cur)

def query_plans(orig_stop_ids, dest_stop_ids):

    with db as cur:

        cur.execute('''
            select distinct on (route_id)

                -- the common part
                route_id,
                route.name
                    as route_name,

                -- from outer (orig_phi and joins)
                orig_phi.serial_no
                    as orig_serial_no,
                orig_phi.it_is_return
                    as orig_is_return,
                orig_phi.stop_id
                    as orig_stop_id,
                stop.name
                    as orig_stop_name,

                -- from inner (dest_phi)
                dest_phi.serial_no
                    as dest_serial_no,
                dest_phi.it_is_return
                    as dest_is_return,
                dest_phi.stop_id
                    as dest_stop_id,
                dest_phi.stop_name
                    as dest_stop_name

            -- the outer
            from
                phi as orig_phi
            left join
                route
            on
                route.id = route_id
            left join
                stop
            on
                stop.id = stop_id

            -- the inner
            inner join (
                select
                    route_id,
                    serial_no,
                    it_is_return,
                    stop_id,
                    stop.name as stop_name
                from
                    phi
                left join
                    stop
                on
                    stop.id = stop_id
                where
                    stop_id in %s
            ) as dest_phi
            using
                (route_id)

            where
                orig_phi.stop_id in %s and
                orig_phi.serial_no < dest_phi.serial_no
            order by
                route_id,
                dest_phi.serial_no-orig_phi.serial_no

            ;
        ''', (tuple(orig_stop_ids), tuple(dest_stop_ids)))

        return all_to_dicts(cur)

if __name__ == '__main__':

    import uniout
    from pprint import pprint

    orig_stops = query_stops(u'台電大樓')
    pprint(orig_stops)

    dest_stops = query_stops(u'西門')
    pprint(dest_stops)

    orig_stop_ids = [s['id'] for s in orig_stops]
    dest_stop_ids = [
        s['id']
        for s in dest_stops
        if s['name'] in (u'西門', u'捷運西門站')
    ]
    pprint(query_plans(orig_stop_ids, dest_stop_ids))

    import sys; sys.exit()

    sync_stops_n_phis_of_route('tp_10723')

    import sys; sys.exit()

    for rids in  _query_route_ids_it():
        print len(rids)

    import sys; sys.exit()

    # merge basic data into db
    sync_routes_on_all_route_indexes()
    sync_stops_n_phis_of_route()
