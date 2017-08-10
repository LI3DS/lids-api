# -*- coding: utf-8 -*-
from flask_restplus import fields
from psycopg2 import sql

from api_li3ds.app import api, Resource, defaultpayload
from api_li3ds.database import Database
from api_li3ds.exc import abort


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


servers_sql = """
    select
        s.srvname as id
        , s.srvname as "name"
        , coalesce(
            (select option_value
             from pg_options_to_table(srvoptions)
             where option_name = 'wrapper'), '')
          as "driver"
        , coalesce(
            (select jsonb_object_agg(option_name, option_value)
             from pg_options_to_table(srvoptions)
             where option_name != 'wrapper'), '{}'::jsonb)
          as "options"
    from pg_catalog.pg_foreign_server s
"""


tables_sql = """
    select
        n.nspname || '.' || c.relname as table
        , s.srvname as server
    from pg_catalog.pg_class c
    join pg_catalog.pg_foreign_table t on t.ftrelid=c.oid
    join pg_catalog.pg_foreign_server s on s.oid=t.ftserver
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
"""


@nsfpc.route('/drivers/', endpoint='foreigndrivers')
class ForeignDrivers(Resource):

    def get(self):
        '''
        Retrieve driver list (multicorn based wrappers)
        '''
        drivers = Database.notices(multicorn_drivers_sql)[-1]
        return drivers.strip('NOTICE: \n').split(',')


@nsfpc.route('/servers/', endpoint='foreignservers')
class ForeignServers(Resource):

    def get(self):
        '''
        Retrieve foreign server list
        '''
        return Database.query_asjson(servers_sql)

    @api.secure
    @nsfpc.expect(foreignpc_server_model)
    def post(self):
        '''
        Create a foreign server
        '''
        drivers = Database.notices(multicorn_drivers_sql)[-1]
        drivers = drivers.strip('NOTICE: \n').split(',')

        if api.payload['driver'] not in drivers:
            return abort(
                404,
                '{} driver does not exist, available drivers are {}'
                .format(api.payload['driver'], drivers))

        options = api.payload['options']
        options.update(wrapper=api.payload['driver'])
        options = {k: str(v) for k, v in options.items()}

        options_sql = sql.SQL(', ').join([
            sql.SQL(' ').join((sql.Identifier(opt), sql.Placeholder(opt)))
            for opt in options
        ])

        req = sql.SQL("""
            create server {name} foreign data wrapper multicorn options (
                {options}
            );
        """).format(name=sql.Identifier(api.payload['name']), options=options_sql)

        Database.rowcount(req, options)

        req = servers_sql + ' where srvname = %(name)s'

        return Database.query_asjson(req, api.payload), 201


@nsfpc.route('/tables/', endpoint='foreigntable')
class ForeignTable(Resource):

    def get(self):
        '''
        Retrieve foreign table list
        '''
        return Database.query_asjson(tables_sql)

    @api.secure
    @nsfpc.expect(foreignpc_table_model)
    def post(self):
        '''
        Create a foreign table
        '''
        payload = defaultpayload(api.payload)

        if len(payload['table'].split('.')) != 2:
            abort(404, 'table should be in the form schema.table ({table})'.format(**payload))

        for server in Database.query_asdict(servers_sql):
            if payload['server'] == server['name']:
                break
        else:
            abort(404, 'no server {}'.format(payload['server']))

        schema_options = {'metadata': 'true'}

        if server['driver'] == 'fdwli3ds.Rosbag':
            if 'topic' not in payload.get('options', {}):
                abort(404, '"topic" option required for Rosbag')
            schema_options.update(topic=payload['options']['topic'])
        elif server['driver'] == 'fdwli3ds.EchoPulse':
            if 'directory' not in payload.get('options', {}):
                abort(404, '"topic" option required for Rosbag')
            schema_options.update(directory=payload['options']['directory'])

        schema_options = {k: str(v) for k, v in schema_options.items()}

        schema, tablename = payload['table'].split('.')

        server_identifier = sql.Identifier(payload['server'])
        schema_identifier = sql.Identifier(schema)
        table_identifier = sql.Identifier(tablename)
        table_schema_identifier = sql.Identifier(tablename + '_schema')

        schema_options_sql = sql.SQL(',').join([
            sql.SQL(' ').join((sql.Identifier(opt), sql.Placeholder(opt)))
            for opt in schema_options
        ])

        req = sql.SQL("""
            create foreign table {schema}.{table_schema} (
                schema text
            )
            server {server} options (
                {options}
            );
            with tmp as (
                select coalesce(max(pcid) + 1, 1) as newid from pointcloud_formats
            )
            insert into pointcloud_formats(pcid, srid, schema)
            select tmp.newid, %(srid)s, schema from {schema}.{table_schema}, tmp
            returning pcid
        """).format(schema=schema_identifier, table_schema=table_schema_identifier,
                    server=server_identifier, options=schema_options_sql)

        parameters = {'srid': payload['srid']}
        parameters.update(schema_options)
        pcid = Database.query_asdict(req, parameters)[0]['pcid']

        req = sql.SQL("drop foreign table {schema}.{table_schema}").format(
                schema=schema_identifier, table_schema=table_schema_identifier)

        Database.rowcount(req)

        options = payload['options']
        options.update(pcid=str(pcid))
        options = {k: str(v) for k, v in options.items()}

        options_sql = sql.SQL(', ').join([
            sql.SQL(' ').join((sql.Identifier(opt), sql.Placeholder(opt)))
            for opt in options
        ])

        req = sql.SQL("""
            create foreign table {schema}.{table} (
                points pcpatch(%(pcid_int)s)
            ) server {server}
                options (
                    {options}
                )
        """).format(schema=schema_identifier, table=table_identifier,
                    server=server_identifier, options=options_sql)

        parameters = {'pcid': str(pcid), 'pcid_int': pcid}
        parameters.update(options)
        Database.rowcount(req, parameters)

        req = tables_sql + ' where c.relname = %(tablename)s and s.srvname = %(server)s' \
                           ' and n.nspname = %(schema)s'

        parameters = {'schema': schema, 'tablename': tablename, 'server': payload['server']}

        return Database.query_asjson(req, parameters), 201


@nsfpc.route('/schema', endpoint='foreignschema')
class ForeignSchema(Resource):

    @api.secure
    @nsfpc.expect(foreignpc_schema_model)
    def post(self):
        '''
        Import foreign schema for a rosbag file
        '''

        req = sql.SQL("""
            create schema if not exists {schema};
            select coalesce(max(pcid) + 1, 1) as pcid from pointcloud_formats
        """).format(schema=sql.Identifier(api.payload['schema']))
        pcid = Database.query_asdict(req)[0]['pcid']

        identifiers = {k: sql.Identifier(v) for k, v in api.payload.items()}
        req = sql.SQL("""
            import foreign schema {rosbag} limit to (pointcloud_formats)
            from server {server} into {schema} options (pcid %(pcid)s);

            insert into pointcloud_formats select pcid, srid, schema
            from {schema}.pointcloud_formats;

            import foreign schema {rosbag} except (pointcloud_formats)
            from server {server} into {schema} options (pcid %(pcid)s)
        """).format(**identifiers)
        Database.rowcount(req, {'pcid': str(pcid)})

        return "foreign schema imported", 201
