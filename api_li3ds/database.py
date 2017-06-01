# -*- coding: utf-8 -*-
from itertools import chain
from functools import wraps

from psycopg2 import connect
from psycopg2.extras import NamedTupleCursor, Json, register_default_jsonb
from psycopg2 import Error as PsycoError
from psycopg2.extensions import register_adapter

from flask import current_app
from flask_restplus import abort

# adapt python dict to postgresql json type
register_adapter(dict, Json)

# register the jsonb type
register_default_jsonb()


def pgexceptions(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except PsycoError as exc:
            current_app.logger.error(exc.pgerror or exc.args)
            if current_app.debug:
                msg = '{} - {}'.format(exc.diag.message_detail, exc.diag.message_primary)
                return abort(404, msg)
            return abort(404, 'Database Error')
        return func(*args, **kwargs)
    return decorated


class Database():
    '''
    Database object used as a global connection object to the db
    '''
    db = None

    @classmethod
    def _query(cls, query, parameters=None, rowcount=None):
        '''
        Performs a query and returns results as a named tuple
        '''
        cur = cls.db.cursor()
        cur.execute(query, parameters)
        current_app.logger.debug(
            'query: {}, rowncount: {}'.format(query, cur.rowcount)
        )
        if rowcount:
            yield cur.rowcount
            return
        for row in cur:
            yield row

    @classmethod
    def rowcount(cls, query, parameters=None):
        '''
        Iterates over results and returns namedtuples
        '''
        rc = next(cls._query(query, parameters=parameters, rowcount=True))
        if rc <= 0:
            return 0
        return rc

    @classmethod
    def query_asdict(cls, query, parameters=None):
        '''
        Iterates over results and returns namedtuples
        '''
        return [
            line._asdict()
            for line in cls._query(query, parameters=parameters)
        ]

    @classmethod
    def query_asjson(cls, query, parameters=None):
        '''
        Wrap query with a json serialization directly in postgres
        and return
        '''
        return [
            line[0] for line in
            cls._query(
                "select row_to_json(t) from ({}) as t"
                .format(query), parameters=parameters
            )
        ]

    @classmethod
    def query_aslist(cls, query, parameters=None):
        '''
        Iterates over results and returns values in a flat list
        (usefull if one column only)
        '''
        return list(chain(*cls._query(query, parameters=parameters)))

    @classmethod
    def query(cls, query, parameters=None):
        '''
        Iterates over results and returns values in a list
        '''
        return list(cls._query(query, parameters=parameters))

    @classmethod
    def init_app(cls, app):
        '''
        Initialize db session lazily
        '''
        cls.db = connect(
            "postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_name}"
            .format(**app.config),
            cursor_factory=NamedTupleCursor,
        )
        # autocommit mode for performance (we don't need transaction)
        cls.db.autocommit = True
