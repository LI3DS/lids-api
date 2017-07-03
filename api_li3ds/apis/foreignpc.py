# -*- coding: utf-8 -*-
from flask_restplus import fields

from api_li3ds.app import api, Resource, defaultpayload
from api_li3ds.database import Database


nsfpc = api.namespace(
    'foreignpc',
    description='foreign objects used for pointcloud data')


foreignpc_table_model = nsfpc.model(
    'foreign table creation',
    {
        'table': fields.String(required=True),
        'server': fields.String(required=True),
        'options': fields.Raw(),
        'srid': fields.Integer(required=False, default=0)
    })


foreignpc_server_model = nsfpc.model(
    'foreign server creation',
    {
        'name': fields.String(description='name for the foreign server'),
        'options': fields.Raw(description='foreign server creation options'),
        'driver': fields.String(required=True)
    })

foreignpc_schema_model = nsfpc.model(
    'foreign schema import for rosbags',
    {
        'schema': fields.String(required=True, description='local schema name'),
        'rosbag': fields.String(required=True, description='rosbag filename'),
        'server': fields.String(required=True)
    })


multicorn_drivers_sql = """
    do $$
    import fdwli3ds
    import inspect
    from multicorn import ForeignDataWrapper
    plpy.notice(','.join([
        'fdwli3ds.' + name
        for name, cl in inspect.getmembers(fdwli3ds,
            lambda x: inspect.isclass(x) and ForeignDataWrapper in x.mro()[1:]
    )]))
    $$ language plpython2u;
"""


@nsfpc.route('/drivers', endpoint='foreigndrivers')
class ForeignDrivers(Resource):

    def get(self):
        '''
        Retrieve driver list (multicorn based wrappers)
        '''
        drivers = Database.notices(multicorn_drivers_sql)[-1]
        return drivers.strip('NOTICE: \n').split(',')


@nsfpc.route('/servers', endpoint='foreignservers')
class ForeignServers(Resource):

    def get(self):
        '''
        Retrieve foreign server list
        '''
        return Database.query_asjson("""
            select
                s.srvname as "name"
                , case when srvoptions is null
                    then '' else (
                    select option_value
                    from pg_options_to_table(srvoptions)
                    where option_name = 'wrapper')
                  end as "driver"
                , case when srvoptions is null
                    then to_jsonb(''::text) else (
                    select jsonb_object_agg(option_name, option_value)
                    from pg_options_to_table(srvoptions)
                    where option_name != 'wrapper')
                  end as "options"
            from pg_catalog.pg_foreign_server s
                join pg_catalog.pg_foreign_data_wrapper f on f.oid=s.srvfdw
            left join pg_description d
                on d.classoid = s.tableoid
                and d.objoid = s.oid and d.objsubid = 0
            """)

    @api.secure
    @nsfpc.expect(foreignpc_server_model)
    def post(self):
        '''
        Create a foreign server
        '''
        drivers = Database.notices(multicorn_drivers_sql)[-1]
        drivers = drivers.strip('NOTICE: \n').split(',')

        if api.payload['driver'] not in drivers:
            return api.abort(
                404,
                '{} driver does not exists, available drivers are {}'
                .format(api.payload['driver'], drivers))

        api.payload.update(
            options='\n'.join([
                ", {} '{}'".format(key, val)
                for key, val in api.payload['options'].items()
            ])
        )
        req = """
            create server {name} foreign data wrapper multicorn options (
                wrapper '{driver}'
                {options}
            );
            """.format(**api.payload)

        Database.rowcount(req)

        return "foreign server created", 201


@nsfpc.route('/table', endpoint='foreigntable')
class ForeignTable(Resource):

    @api.secure
    @nsfpc.expect(foreignpc_table_model)
    def post(self):
        '''
        Create a foreign table
        '''
        payload = defaultpayload(api.payload)
        # create foreign table for schema
        if len(payload['table'].split('.')) != 2:
            api.abort(404, 'table should be in the form schema.table')

        schema, tablename = payload['table'].split('.')

        options = '\n'.join([
            ", {} '{}'".format(key, val)
            for key, val in payload.get('options', {}).items()
        ])

        payload.update(
            schema=schema,
            tablename=tablename,
            options=options)

        pcid = Database.query_asdict(
            """
            create foreign table {schema}.{tablename}_schema (
                schema text
            )
            server {server} options (metadata 'true');

            with tmp as (
                select coalesce(max(pcid) + 1, 0) as newid from pointcloud_formats
            )
            insert into pointcloud_formats(pcid, srid, schema)
            select tmp.newid, %(srid)s, schema from {schema}.{tablename}_schema, tmp
            returning pcid
            """
            .format(**payload), (payload))[0]['pcid']

        Database.rowcount(
            "drop foreign table {schema}.{tablename}_schema"
            .format(**payload))

        payload.update(pcid=pcid)

        Database.rowcount("""
            create foreign table {schema}.{tablename} (
                points pcpatch({pcid})
            ) server {server}
                options (
                    pcid '{pcid}'
                    {options}
                )
            """.format(**payload)
        )
        return "foreign table created", 201


@nsfpc.route('/schema', endpoint='foreignschema')
class ForeignSchema(Resource):

    @api.secure
    @nsfpc.expect(foreignpc_schema_model)
    def post(self):
        '''
        Import foreign schema for a rosbag file
        '''
        pcid = Database.query_asdict("""
            create schema if not exists "{schema}";

            select coalesce(max(pcid) + 1, 0) as pcid from pointcloud_formats
        """.format(**api.payload))[0]['pcid']

        req = """
            import foreign schema "{rosbag}" limit to (pointcloud_formats)
            from server {server} into "{schema}" options (pcid '{pcid}');

            insert into pointcloud_formats select pcid, srid, schema
            from "{schema}".pointcloud_formats;

            import foreign schema "{rosbag}" except (pointcloud_formats)
            from server {server} into "{schema}" options (pcid '{pcid}')
        """.format(pcid=pcid, **api.payload)
        Database.rowcount(req)

        return "foreign schema imported", 201