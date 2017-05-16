# -*- coding: utf-8 -*-
from flask_restplus import fields

from api_li3ds.database import Database
from api_li3ds.app import api, Resource, defaultpayload
from .session import session_model


nsproject = api.namespace('projects', description='projects related operations')


class GeometryInput(fields.String):
    __schema_format__ = 'geometry_string'
    __schema_example__ = 'srid=4326;polygon((1 1, 2 2, 3 3, 1 1))'


class GeometryOutput(fields.String):
    __schema_format__ = 'geometry_wkb'
    __schema_example__ = '01030000...'


project_model_post = nsproject.model('Project Model Post', {
    'name': fields.String,
    'extent': GeometryInput(default=None),
    'timezone': fields.String(default="Europe/Paris")
})

project_model = nsproject.inherit('Project Model', project_model_post, {
    'id': fields.Integer(description='the unique project identifier'),
    'extent': GeometryOutput
})


@nsproject.route('/', endpoint='projects')
class Projects(Resource):

    @nsproject.marshal_with(project_model)
    def get(self):
        '''List all projects'''
        return Database.query_asjson("select * from li3ds.project")

    @api.secure
    @nsproject.expect(project_model_post)
    @nsproject.marshal_with(project_model, mask='id')
    def post(self):
        '''
        Create a project.
        '''
        return Database.query_asdict(
            "insert into li3ds.project (name, timezone, extent) "
            "values (%(name)s, %(timezone)s, %(extent)s::geometry) "
            "returning id",
            defaultpayload(api.payload)
        ), 201


@nsproject.route('/<string:name>/', endpoint='project')
@nsproject.response(404, 'Project not found')
@nsproject.param('name', 'The project name')
class OneProject(Resource):

    @nsproject.marshal_with(project_model)
    def get(self, name):
        '''Get a project given its name'''
        res = Database.query_asjson("select * from li3ds.project where name=%s", (name,))
        if not res:
            nsproject.abort(404, 'Project not found')
        return res

    @api.secure
    @nsproject.response(410, 'Session deleted')
    @nsproject.marshal_with(project_model, mask='id')
    def delete(self, name):
        '''
        Delete a project.
        '''
        res = Database.query_asjson("select * from li3ds.project where name=%s", (name,))
        if not res:
            nsproject.abort(404, 'Project not found')
        Database.query_aslist("delete from li3ds.project where id=%s", (name,))
        return '', 410


@nsproject.route('/<string:name>/sessions/', endpoint='project_sessions')
@nsproject.response(404, 'Project not found')
@nsproject.param('name', 'The project name')
class Sessions(Resource):

    @nsproject.marshal_with(session_model)
    def get(self, name):
        '''List all sessions for a given project'''
        res = Database.query_asjson("select * from li3ds.project where name=%s", (name,))
        if not res:
            nsproject.abort(404, 'Project not found')
        return Database.query_asjson(
            """select s.* from li3ds.session s
            join li3ds.project p on s.project=p.id where p.name=%s
            """, (name,)
        )
