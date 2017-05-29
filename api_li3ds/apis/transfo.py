# -*- coding: utf-8 -*-
from flask_restplus import fields

from api_li3ds.app import api, Resource, defaultpayload
from api_li3ds.database import Database
from api_li3ds import fields as li3ds_fields

nstf = api.namespace('transfos', description='transformations related operations')


transfo_model_post = nstf.model(
    'Transformation Model Post',
    {
        'name': fields.String,
        'source': fields.Integer,
        'target': fields.Integer,
        'transfo_type': fields.Integer,
        'description': fields.String,
        'parameters': fields.Raw,
        'tdate': li3ds_fields.DateTime(dt_format='iso8601'),
        'validity_start': li3ds_fields.DateTime(dt_format='iso8601'),
        'validity_end': li3ds_fields.DateTime(dt_format='iso8601'),
    })

transfo_model = nstf.inherit(
    'Transformation Model',
    transfo_model_post,
    {
        'id': fields.Integer
    })


transfotype_model_post = nstf.model(
    'Transformation type Model Post',
    {
        'name': fields.String,
        'description': fields.String,
        'func_signature': fields.List(fields.String),
    })

transfotype_model = nstf.inherit(
    'Transformation type Model',
    transfotype_model_post,
    {
        'id': fields.Integer
    })


@nstf.route('/', endpoint='transfos')
class Transfo(Resource):

    @nstf.marshal_with(transfo_model)
    def get(self):
        '''List all transformations'''
        return Database.query_asjson("select * from li3ds.transfo")

    @api.secure
    @nstf.expect(transfo_model_post)
    @nstf.marshal_with(transfo_model)
    @nstf.response(201, 'Transformation created')
    def post(self):
        '''Create a transformation between referentials'''
        return Database.query_asdict(
            """
            insert into li3ds.transfo (name, source, target, transfo_type, description,
                                       parameters, tdate, validity_start, validity_end)
            values (%(name)s, %(source)s, %(target)s, %(transfo_type)s, %(description)s,
                    %(parameters)s, %(tdate)s, %(validity_start)s, %(validity_end)s)
            returning *
            """, defaultpayload(api.payload)
        ), 201


@nstf.route('/<int:id>/', endpoint='transfo')
@nstf.response(404, 'Transformation not found')
class OneTransfo(Resource):

    @nstf.marshal_with(transfo_model)
    def get(self, id):
        '''Get one transformation given its identifier'''
        res = Database.query_asjson(
            "select * from li3ds.transfo where id=%s", (id,)
        )
        if not res:
            nstf.abort(404, 'Transformation not found')
        return res

    @api.secure
    @nstf.response(410, 'Transformation deleted')
    def delete(self, id):
        '''Delete a transformation given its identifier'''
        res = Database.rowcount("delete from li3ds.transfo where id=%s", (id,))
        if not res:
            nstf.abort(404, 'Transformation not found')
        return '', 410


@nstf.route('/types/', endpoint='transfotypes')
class TransfoType(Resource):

    @nstf.marshal_with(transfotype_model)
    def get(self):
        '''List all transformation types'''
        return Database.query_asjson("select * from li3ds.transfo_type")

    @api.secure
    @nstf.expect(transfotype_model_post)
    @nstf.marshal_with(transfotype_model)
    @nstf.response(201, 'Transformation type created')
    def post(self):
        '''Create a transformation type'''
        return Database.query_asdict(
            """
            insert into li3ds.transfo_type (name, description, func_signature)
            values (%(name)s,%(description)s,%(func_signature)s)
            returning *
            """,
            defaultpayload(api.payload)
        ), 201


@nstf.route('/types/<int:id>/', endpoint='transfotype')
@nstf.response(404, 'Transformation type not found')
class OneTransfoType(Resource):

    @nstf.marshal_with(transfotype_model)
    def get(self, id):
        '''Get one transformation type given its identifier'''
        res = Database.query_asjson(
            "select * from li3ds.transfo_type where id=%s", (id,)
        )
        if not res:
            nstf.abort(404, 'Transformation type not found')
        return res

    @api.secure
    @nstf.response(410, 'Transformation type deleted')
    def delete(self, id):
        '''Delete a transformation type given its identifier'''
        res = Database.rowcount("delete from li3ds.transfo_type where id=%s", (id,))
        if not res:
            nstf.abort(404, 'Transformation type not found')
        return '', 410
