# -*- coding: utf-8 -*-
import psycopg2
import flask_restplus
import functools

from flask import current_app


def abort(status_code, http_msg, log_msg=None):
    """abort the current request, loggin an error message
    """
    if not log_msg:
        log_msg = http_msg
    current_app.logger.error(log_msg)
    return flask_restplus.abort(status_code, http_msg)


def abort_pgexc(status_code, exc):
    """abort on a postgres exception
    """
    http_msg = '{} - {}'.format(exc.diag.message_detail, exc.diag.message_primary) if \
               current_app.debug else 'Database Error'
    return abort(status_code, http_msg, exc.pgerror or exc.args)


def pgexceptions(func):
    @functools.wraps(func)
    def decorated(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except psycopg2.IntegrityError as exc:
            return abort_pgexc(404, exc)
        except psycopg2.Error as exc:
            return abort_pgexc(400, exc)
    return decorated
