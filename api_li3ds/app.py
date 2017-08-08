# -*- coding: utf-8 -*-
from functools import wraps
from collections import defaultdict

from flask import request, current_app
from flask_restplus import Api, Resource as OrigResource

from api_li3ds.exc import pgexceptions

HEADER_API_KEY = 'X-API-KEY'


class Resource(OrigResource):
    # add a postgresql exception decorator for all api methods
    method_decorators = [pgexceptions]


def defaultpayload(payload):
    """Use a default dict to add a None value
    and avoid a KeyError on sql request interpolation
    """
    newpayload = defaultdict(lambda: None)
    newpayload.update(payload)
    return newpayload


class Li3dsApi(Api):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def secure(self, func):
        '''Enforce authentication'''

        @wraps(func)
        def wrapper(*args, **kwargs):
            if HEADER_API_KEY not in request.headers:
                self.abort(401, '{} required'.format(HEADER_API_KEY))
            apikey = request.headers[HEADER_API_KEY]
            if apikey != current_app.config['HEADER_API_KEY']:
                # for now only one global api key form config file
                self.abort(401, 'Unauthorized')
            else:
                return func(*args, **kwargs)

        return wrapper


api = Li3dsApi(
    version='1.0', title='Large Input 3D System API',
    description='API for accessing LI3DS metadata',
    validate=True,
    authorizations={
        'apikey': {
            'type': 'apiKey',
            'in': 'header',
            'name': HEADER_API_KEY
        }
    }
)


def init_apis():
    from api_li3ds.apis.project import nsproject
    from api_li3ds.apis.session import nssession
    from api_li3ds.apis.platform import nspfm
    from api_li3ds.apis.itowns import nsitowns
    from api_li3ds.apis.datasource import nsds
    from api_li3ds.apis.sensor import nssensor
    from api_li3ds.apis.referential import nsrf
    from api_li3ds.apis.transfo import nstf
    from api_li3ds.apis.transfotree import nstft
    from api_li3ds.apis.foreignpc import nsfpc
