# -*- coding: utf-8 -*-
from flask_restplus import fields

from api_li3ds.app import api, Resource, defaultpayload
from api_li3ds.database import Database
from api_li3ds import fields as li3ds_fields

nsrf = api.namespace('referentials', description='referentials related operations')

referential_model_post = nsrf.model(
    'Referential Model Post',
    {
        'name': fields.String,
        'description': fields.String,
        'srid': fields.Integer,
        'sensor': fields.Integer,
    })

referential_model = nsrf.inherit(
    'Referential Model',
    referential_model_post,
    {
        'id': fields.Integer
    })


transfo_model = nsrf.model(
    'Transfo Model',
    {
        'id': fields.Integer,
        'source': fields.Integer,
        'target': fields.Integer,
        'transfo_type': fields.Integer,
        'description': fields.String,
        'parameters': fields.Raw,
        'tdate': li3ds_fields.DateTime(dt_format='iso8601'),
        'validity_start': li3ds_fields.DateTime(dt_format='iso8601', default=None),
        'validity_end': li3ds_fields.DateTime(dt_format='iso8601', default=None),
    })


@nsrf.route('/', endpoint='referentials')
class Referential(Resource):

    @nsrf.marshal_with(referential_model)
    def get(self):
        '''List Referentials'''
        return Database.query_asjson("select * from li3ds.referential")

    @api.secure
    @nsrf.expect(referential_model_post)
    @nsrf.marshal_with(referential_model)
    @nsrf.response(201, 'Platform created')
    def post(self):
        '''Create a referential'''
        return Database.query_asdict(
            "insert into li3ds.referential (name, description, srid, sensor) "
            "values (%(name)s, %(description)s, %(srid)s, %(sensor)s) "
            "returning *",
            defaultpayload(api.payload)
        ), 201


@nsrf.route('/<int:id>/', endpoint='referential')
@nsrf.response(404, 'Referential not found')
class OneReferential(Resource):

    @nsrf.marshal_with(referential_model)
    def get(self, id):
        '''Get one referential given its identifier'''
        res = Database.query_asjson(
            "select * from li3ds.referential where id=%s", (id,)
        )
        if not res:
            nsrf.abort(404, 'Referential not found')
        return res

    @api.secure
    @nsrf.response(410, 'Referential deleted')
    def delete(self, id):
        '''Delete a referential given its identifier'''
        res = Database.rowcount("delete from li3ds.referential where id=%s", (id,))
        if not res:
            nsrf.abort(404, 'referential not found')
        return '', 410
