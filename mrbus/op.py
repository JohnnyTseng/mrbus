#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mrbus.conn import db
from mrbus.model import merge_routes_on_route_indexes

def create_tables():

    # route
    #
    # 1. id -> tp_10723 / nt_114
    # 2. name
    # 3. updated_ts
    # 4. created_ts
    #

    # stop
    #
    # 1. id (serial)
    # 2. name
    # 3. updated_ts
    # 4. created_ts
    #

    # interval
    #
    # 1. route_id
    # 2. stop_id
    # 3. interval_min
    # 4. updated_ts
    # 5. created_ts
    #

    # serial  : 1 to 2147483647
    # smallint: -32768 to +32767
    # int     : -2147483648 to +2147483647
    # ref: http://www.postgresql.org/docs/9.1/static/datatype-numeric.html

    with db as cur:

        # route

        cur.execute('''
            create table route (
                id         text primary key,
                name       text,
                updated_ts timestamp,
                created_ts timestamp
            )
        ''')

        cur.execute('create index on route (name)')

        # stop

        cur.execute('''
            create table stop (
                id         serial primary key,
                name       text,
                updated_ts timestamp,
                created_ts timestamp
            )
        ''')

        cur.execute('create index on stop (name)')

        # phi (Φ)
        # because the alphabet looks like a route (|) corsses over a stop (o),
        # lol.

        cur.execute('''
            create table phi (
                route_id     text references route (id),
                stop_id      int references stop (id),
                serial_no    smallint,
                it_is_return bool,
                waiting_min  smallint,
                interval_min smallint,
                updated_ts   timestamp,
                created_ts   timestamp,
                primary key (route_id, stop_id, serial_no)
            )
        ''')

        cur.execute('create index on phi (stop_id)')

def drop_tables():

    with db as cur:
        cur.execute('drop table phi')
        cur.execute('drop table stop')
        cur.execute('drop table route')

if __name__ == '__main__':

    import clime
    clime.start()
