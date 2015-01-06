#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mrbus.conn import db
from mrbus.model import (
    sync_routes_on_all_route_indexes,
    sync_stops_n_phis_of_all_routes
)

def create_tables():

    # route
    #
    # 1. id -> tp_10723 / nt_114
    # 2. name
    # 3. on_index
    # 4. updated_ts
    # 5. created_ts
    #

    # stop
    #
    # 1. id (serial)
    # 2. name
    # 3. created_ts
    #

    # phi (Φ) - stops on routes
    #
    # because the alphabet looks like a route (|) corsses over a stop (o),
    # lol.
    #
    # 1. route_id
    # 2. serial_no
    # 3. it_is_return
    # 4. stop_id
    # 5. status_code
    # 6. waiting_min
    # 7. interval_min
    # 8. updated_ts
    # 9. created_ts
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
                on_index   bool default true,
                updated_ts timestamp,
                created_ts timestamp
            )
        ''')

        cur.execute('create index on route (name)')
        cur.execute('create index on route (on_index)')

        # stop

        cur.execute('''
            create table stop (
                id         serial primary key,
                name       text,
                created_ts timestamp
            )
        ''')

        cur.execute('create index on stop (name)')

        # phi

        cur.execute('''
            create table phi (
                route_id     text references route (id),
                serial_no    smallint,
                it_is_return bool,
                stop_id      int references stop (id),
                status_code  smallint,
                waiting_min  smallint,
                interval_min numeric(5, 2),
                updated_ts   timestamp,
                created_ts   timestamp,
                primary key (route_id, serial_no)
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
    clime.start(debug=True)
