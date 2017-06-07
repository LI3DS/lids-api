# -*- coding: utf-8 -*-
from flask import make_response
from flask_restplus import fields

from api_li3ds.app import api, Resource, defaultpayload
from api_li3ds.database import Database
from api_li3ds import dot

nstft = api.namespace('transfotrees', description='transformation trees related operations')

transfotree_model_post = nstft.model(
    'Transformation tree Model Post',
    {
        'name': fields.String,
        'sensor': fields.Integer,
        'owner': fields.String,
        'transfos': fields.List(fields.Integer),
    })

transfotree_model = nstft.inherit(
    'Transformation tree Model',
    transfotree_model_post,
    {
        'id': fields.Integer
    })


@nstft.route('/', endpoint='transfotrees')
class TransfoTree(Resource):

    @nstft.marshal_with(transfotree_model)
    def get(self):
        '''List all transformation trees'''
        return Database.query_asjson("select * from li3ds.transfo_tree")

    @api.secure
    @nstft.expect(transfotree_model_post)
    @nstft.marshal_with(transfotree_model)
    @nstft.response(201, 'Transformation created')
    def post(self):
        '''Create a transformation between referentials'''
        return Database.query_asdict(
            """
            insert into li3ds.transfo_tree (name, sensor, owner, transfos)
            values (%(name)s,%(sensor)s,%(owner)s,%(transfos)s)
            returning *
            """,
            defaultpayload(api.payload)
        ), 201


@nstft.route('/<int:id>/', endpoint='transfotree')
@nstft.response(404, 'Transformation tree not found')
class OneTransfoTree(Resource):

    @nstft.marshal_with(transfotree_model)
    def get(self, id):
        '''Get one transformation given its identifier'''
        res = Database.query_asjson(
            "select * from li3ds.transfo_tree where id=%s", (id,)
        )
        if not res:
            nstft.abort(404, 'Transformation tree not found')
        return res

    @api.secure
    @nstft.response(410, 'Transformation deleted')
    def delete(self, id):
        '''Delete a transformation given its identifier'''
        res = Database.rowcount("delete from li3ds.transfo_tree where id=%s", (id,))
        if not res:
            nstft.abort(404, 'Transformation tree not found')
        return '', 410


@nstft.route('/<int:id>/dot/', endpoint='transfotree_dot')
@nstft.param('id', 'The platform config identifier')
class TransfoTreeDot(Resource):

    def get(self, id):
        '''Get a preview for this transfo tree as dot

        Nodes are referentials and edges are tranformations between referentials.
        Blue arrows represents connections between sensors (or sensor groups).
        '''
        graph = dot.transfo_tree(id)
        if not graph:
            nstft.abort(404, 'Transformation tree not found')

        response = make_response(graph.source)
        response.headers['content-type'] = 'text/plain'
        response.mimetype = 'text/plain'
        return response


@nstft.route('/<int:id>/preview/', endpoint='transfotree_preview')
@nstft.param('id', 'The platform config identifier')
class TransfoTreePreview(Resource):

    def get(self, id):
        '''Get a preview for this transfo tree as png

        Nodes are referentials and edges are tranformations between referentials.
        Blue arrows represents connections between sensors (or sensor groups).
        '''
        graph = dot.transfo_tree(id)
        if not graph:
            nstft.abort(404, 'Transformation tree not found')

        response = make_response(graph.pipe("png"))
        response.headers['content-type'] = 'image/png'
        response.mimetype = 'image/png'
        return response
