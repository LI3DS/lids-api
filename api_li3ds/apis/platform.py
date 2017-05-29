# -*- coding: utf-8 -*-
from flask import make_response
from flask_restplus import fields

from api_li3ds.app import api, Resource, defaultpayload
from api_li3ds.database import Database
from api_li3ds import dot
from api_li3ds import fields as li3ds_fields

from .sensor import sensor_model

nspfm = api.namespace('platforms', description='platforms related operations')

platform_model_post = nspfm.model(
    'Platform Model Post',
    {
        'name': fields.String,
        'description': fields.String,
        'start_time': li3ds_fields.DateTime(dt_format='iso8601', default=None),
        'end_time': li3ds_fields.DateTime(dt_format='iso8601', default=None),
    })

platform_model = nspfm.inherit(
    'Platform Model',
    platform_model_post,
    {
        'id': fields.Integer
    })

platform_config_post = nspfm.model(
    'Platform Config Post',
    {
        'name': fields.String,
        'owner': fields.String,
        'transfo_trees': fields.List(fields.Integer)
    })

platform_config = nspfm.inherit(
    'Platform Config',
    platform_config_post,
    {
        'id': fields.Integer,
    })


@nspfm.route('/', endpoint='platforms')
class Platforms(Resource):

    @nspfm.marshal_with(platform_model)
    def get(self):
        '''List platforms'''
        return Database.query_asjson("select * from li3ds.platform")

    @api.secure
    @nspfm.expect(platform_model_post)
    @nspfm.marshal_with(platform_model)
    @nspfm.response(201, 'Platform created')
    def post(self):
        '''Create a platform'''
        return Database.query_asdict(
            "insert into li3ds.platform (name, description, start_time, end_time) "
            "values (%(name)s, %(description)s, %(start_time)s, %(end_time)s) "
            "returning *",
            defaultpayload(api.payload)
        ), 201


@nspfm.route('/<int:id>/', endpoint='platform')
@nspfm.response(404, 'Platform not found')
class OnePlatform(Resource):

    @nspfm.marshal_with(platform_model)
    def get(self, id):
        '''Get one platform given its identifier'''
        res = Database.query_asjson(
            "select * from li3ds.platform where id=%s", (id,)
        )
        if not res:
            nspfm.abort(404, 'Platform not found')
        return res

    @api.secure
    @nspfm.response(410, 'Platform deleted')
    def delete(self, id):
        '''Delete a platform given its identifier'''
        res = Database.rowcount("delete from li3ds.platform where id=%s", (id,))
        if not res:
            nspfm.abort(404, 'Platform not found')
        return '', 410


@nspfm.route('/<int:id>/configs/', endpoint='platform_configs')
class PlatformConfigs(Resource):

    @nspfm.marshal_with(platform_config)
    def get(self, id):
        '''List all platform configurations'''
        return Database.query_asjson(
            "select * from li3ds.platform_config where platform = %s", (id,)
        )

    @api.secure
    @nspfm.expect(platform_config_post)
    @nspfm.marshal_with(platform_config)
    def post(self, id):
        '''Create a new platform configuration'''
        return Database.query_asdict(
            "insert into li3ds.platform_config (name, owner, platform, transfo_trees) "
            "values (%(name)s, %(owner)s, {}, %(transfo_trees)s) "
            "returning *".format(id),
            defaultpayload(api.payload)
        ), 201


@nspfm.route('/configs/<int:id>/', endpoint='platform_config')
@nspfm.param('id', 'The platform config identifier')
class OnePlatformConfig(Resource):

    def get(self, id):
        '''Get a platform configuration given its identifier'''
        return Database.query_asjson(
            "select * from li3ds.platform_config where id = %s", (id,)
        )

    @api.secure
    @nspfm.response(410, 'Platform configuration deleted')
    def delete(self, id):
        '''Delete a platform configuration given its identifier'''
        res = Database.rowcount("delete from li3ds.platform_config where id=%s", (id,))
        if not res:
            nspfm.abort(404, 'Platform configuration not found')
        return '', 410


@nspfm.route('/configs/<int:id>/dot/', endpoint='platform_config_dot')
@nspfm.param('id', 'The platform config identifier')
class PlatformConfigDot(Resource):

    def get(self, id):
        '''Get a preview for this platform configuration as dot

        Nodes are referentials and edges are tranformations between referentials.
        Blue arrows represents connections between sensors (or sensor groups).
        '''
        graph = dot.platform_config(id)
        if not graph:
            nspfm.abort(404, 'Platform configuration not found')

        response = make_response(graph.source)
        response.headers['content-type'] = 'text/plain'
        response.mimetype = 'text/plain'
        return response


@nspfm.route('/configs/<int:id>/preview/', endpoint='platform_config_preview')
@nspfm.param('id', 'The platform config identifier')
class PlatformConfigPreview(Resource):

    def get(self, id):
        '''Get a preview for this platform configuration as png

        Nodes are referentials and edges are tranformations between referentials.
        Blue arrows represents connections between sensors (or sensor groups).
        '''
        graph = dot.platform_config(id)
        if not graph:
            nspfm.abort(404, 'Platform configuration not found')

        response = make_response(graph.pipe("png"))
        response.headers['content-type'] = 'image/png'
        response.mimetype = 'image/png'
        return response


@nspfm.route('/configs/<int:id>/sensors/', endpoint='platform_config_sensors')
@nspfm.param('id', 'The platform config identifier')
class PlatformConfigSensors(Resource):

    @nspfm.marshal_with(sensor_model)
    def get(self, id):
        '''Get all sensors used in a given platform configuration'''
        return Database.query_asjson("""
            select
               distinct s.*
            from li3ds.platform_config pf
            join li3ds.transfo_tree tt on tt.id = ANY(pf.transfo_trees)
            , lateral unnest(tt.transfos) as tid
            join li3ds.transfo t on t.id = tid
            join li3ds.referential r on r.id = t.source or r.id = t.target
            join li3ds.sensor s on s.id = r.sensor
            where pf.id = %s
            """, (id,))
