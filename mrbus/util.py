#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = ['db', 'create_tables', 'drop_tables']

import psycopg2
from getpass import getuser
from mosql.db import Database

db = Database(psycopg2, user=getuser())

def create_tables():

    # route
    #
    # 1. id
    # 2. name
    # 3. updated_ts
    # 4. created_ts
    # 5. json
    #

    # stop
    #
    # 1. id
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

    with db as cur:

        cur.execute('''
            create table route (
                id         text primary key,
                name       text,
                updated_ts timestamp,
                created_ts timestamp
            )
        ''')

        cur.execute('''
            create table stop (
                id         serial primary key,
                name       text,
                updated_ts timestamp,
                created_ts timestamp
            )
        ''')

        cur.execute('''
            create table interval (
                route_id     text references route (id),
                stop_id      int  references stop (id),
                interval_min smallint,
                updated_ts   timestamp,
                created_ts   timestamp,
                primary key (route_id, stop_id)
            )
        ''')

        cur.execute('''
            create index on interval (stop_id)
        ''')

def drop_tables():

    with db as cur:
        cur.execute('drop table interval')
        cur.execute('drop table stop')
        cur.execute('drop table route')

def escape_like_operand(s):
    return s.replace('\\', '\\\\').replce('_', '\_').replace('%', '\%')

if __name__ == '__main__':

    import clime
    clime.start()
