# -*- coding: utf-8 -*-
from flask_restplus import fields

from api_li3ds.app import api, Resource, defaultpayload
from api_li3ds.database import Database

nsimage = api.namespace('images', description='images related operations')

image_model_post = nsimage.model('Image Model Post', {
    'uri': fields.String,
    'exif': fields.Raw,
    'etime': fields.DateTime(dt_format='iso8601'),
    'datasource': fields.Integer(required=True)
})

image_model = nsimage.inherit('Image Model', image_model_post, {
    'id': fields.Integer,
})


@nsimage.route('/', endpoint='images')
class Images(Resource):

    @nsimage.marshal_with(image_model)
    def get(self):
        '''Get all images'''
        return Database.query_asjson("select * from li3ds.image")

    @api.secure
    @nsimage.expect(image_model_post)
    @nsimage.marshal_with(image_model)
    @nsimage.response(201, 'Image created')
    def post(self):
        '''Create an image'''
        return Database.query_asdict(
            """
            insert into li3ds.image (uri, exif, etime, datasource)
            values (%(uri)s, %(exif)s, %(etime)s, %(datasource)s)
            returning *
            """,
            defaultpayload(api.payload)
        ), 201


@nsimage.route('/<int:id>/', endpoint='image')
@nsimage.response(404, 'Image not found')
class OneImage(Resource):

    @nsimage.marshal_with(image_model)
    def get(self, id):
        '''Get one image given its identifier'''
        res = Database.query_asjson(
            "select * from li3ds.image where id=%s", (id,)
        )
        if not res:
            nsimage.abort(404, 'image not found')
        return res

    @api.secure
    @nsimage.response(410, 'Image deleted')
    def delete(self, id):
        '''Delete an image given its identifier'''
        res = Database.rowcount("delete from li3ds.image where id=%s", (id,))
        if not res:
            nsimage.abort(404, 'Image not found')
        return '', 410
