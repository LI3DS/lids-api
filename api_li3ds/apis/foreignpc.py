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
        'srid': fields.Integer(required=True, default=0)
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

foreignpc_view_model = nsfpc.model(
    'foreign view creation',
    {
        'view': fields.String(required=True),
        'table': fields.String(required=True),
        'sbet': fields.Boolean,
        'srid': fields.Integer
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
        n.nspname || '.' || c.relname as id
        , n.nspname || '.' || c.relname as table
        , s.srvname as server
    from pg_catalog.pg_class c
    join pg_catalog.pg_foreign_table t on t.ftrelid=c.oid
    join pg_catalog.pg_foreign_server s on s.oid=t.ftserver
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
"""


views_sql = """
    select
        v.schemaname || '.' || v.matviewname as view
        , v.definition as definition
    from pg_catalog.pg_matviews v
"""

_schema_quat = """<?xml version="1.0" encoding="UTF-8"?>
<pc:PointCloudSchema xmlns:pc="http://pointcloud.org/schemas/PC/1.1"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<pc:dimension>
    <pc:position>1</pc:position>
    <pc:size>8</pc:size>
    <pc:name>qw</pc:name>
    <pc:description>quaternion w</pc:description>
    <pc:interpretation>double</pc:interpretation>
</pc:dimension>
<pc:dimension>
    <pc:position>2</pc:position>
    <pc:size>8</pc:size>
    <pc:name>qx</pc:name>
    <pc:description>quaternion x</pc:description>
    <pc:interpretation>double</pc:interpretation>
</pc:dimension>
<pc:dimension>
    <pc:position>3</pc:position>
    <pc:size>8</pc:size>
    <pc:name>qy</pc:name>
    <pc:description>quaternion y</pc:description>
    <pc:interpretation>double</pc:interpretation>
</pc:dimension>
<pc:dimension>
    <pc:position>4</pc:position>
    <pc:size>8</pc:size>
    <pc:name>qz</pc:name>
    <pc:description>quaternion z</pc:description>
    <pc:interpretation>double</pc:interpretation>
</pc:dimension>
{}
<pc:dimension>
    <pc:position>7</pc:position>
    <pc:size>4</pc:size>
    <pc:name>z</pc:name>
    <pc:description>height in meters</pc:description>
    <pc:interpretation>int32</pc:interpretation>
    <pc:scale>0.01</pc:scale>
</pc:dimension>
<pc:dimension>
    <pc:position>8</pc:position>
    <pc:size>8</pc:size>
    <pc:name>time</pc:name>
    <pc:description>seconds of week in GPS time system</pc:description>
    <pc:interpretation>double</pc:interpretation>
</pc:dimension>
</pc:PointCloudSchema>"""

schema_quat_4326 = _schema_quat.format('''<pc:dimension>
    <pc:position>5</pc:position>
    <pc:size>4</pc:size>
    <pc:name>x</pc:name>
    <pc:description>longitude</pc:description>
    <pc:interpretation>int32</pc:interpretation>
    <pc:scale>0.0000001</pc:scale>
</pc:dimension>
<pc:dimension>
    <pc:position>6</pc:position>
    <pc:size>4</pc:size>
    <pc:name>y</pc:name>
    <pc:description>latitude</pc:description>
    <pc:interpretation>int32</pc:interpretation>
    <pc:scale>0.0000001</pc:scale>
</pc:dimension>''')

schema_quat_projected = _schema_quat.format('''<pc:dimension>
    <pc:position>5</pc:position>
    <pc:size>4</pc:size>
    <pc:name>x</pc:name>
    <pc:description>x</pc:description>
    <pc:interpretation>int32</pc:interpretation>
    <pc:scale>0.01</pc:scale>
</pc:dimension>
<pc:dimension>
    <pc:position>6</pc:position>
    <pc:size>4</pc:size>
    <pc:name>y</pc:name>
    <pc:description>y</pc:description>
    <pc:interpretation>int32</pc:interpretation>
    <pc:scale>0.01</pc:scale>
</pc:dimension>''')


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
                400,
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
            abort(400, 'table should be in the form schema.table ({table})'.format(**payload))

        for server in Database.query_asdict(servers_sql):
            if payload['server'] == server['name']:
                break
        else:
            abort(400, 'no server {}'.format(payload['server']))

        schema_options = {'metadata': 'true'}

        if server['driver'] == 'fdwli3ds.Rosbag':
            if 'topic' not in payload.get('options', {}):
                abort(400, '"topic" option required for Rosbag')
            schema_options.update(topic=payload['options']['topic'])
        elif server['driver'] == 'fdwli3ds.EchoPulse':
            if 'directory' not in payload.get('options', {}):
                abort(400, '"directory" option required for EchoPulse')
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


@nsfpc.route('/schema/', endpoint='foreignschema')
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


@nsfpc.route('/views/', endpoint='foreignview')
class ForeignViews(Resource):

    def get(self):
        '''
        Retrieve foreign view list
        '''
        return Database.query_asjson(views_sql)

    @api.secure
    @nsfpc.expect(foreignpc_view_model)
    def post(self):
        '''
        Create a materialized view
        '''
        payload = defaultpayload(api.payload)

        view_parts = payload['view'].split('.')
        if len(view_parts) != 2:
            abort(400, 'view should be in the form schema.view ({view})'.format(**payload))
        view_schema, view = view_parts

        table_parts = payload['table'].split('.')
        if len(table_parts) != 2:
            abort(400, 'table should be in the form schema.table ({table})'.format(**payload))
        table_schema, table = table_parts

        if payload['srid'] is not None:
            if not payload['sbet']:
                abort(400, 'srid cannot be set when sbet is not')
            if payload['srid'] == 0:
                abort(400, 'srid must not be 0')

        if payload['sbet']:
            srid = payload['srid'] or 4326
            schema_quat = schema_quat_4326 if srid == 4326 else schema_quat_projected

            req = '''
                select pcid from pointcloud_formats
                where srid = %(srid)s and schema = %(schema_quat)s
            '''
            res = Database.query_asdict(req, {'schema_quat': schema_quat, 'srid': srid})
            if not res:
                req = '''
                    with tmp as (
                        select coalesce(max(pcid) + 1, 1) as newid from pointcloud_formats
                    )
                    insert into pointcloud_formats(pcid, srid, schema)
                    select tmp.newid, %(srid)s, %(schema_quat)s from tmp
                    returning pcid
                '''
                res = Database.query_asdict(req, {'schema_quat': schema_quat, 'srid': srid})
            pcid = res[0]['pcid']

            select = '''
                with param as (
                    select cos(pc_get(point, 'm_plateformHeading') * 0.5) as t0,
                           sin(pc_get(point, 'm_plateformHeading') * 0.5) as t1,
                           cos(pc_get(point, 'm_roll') * 0.5) as t2,
                           sin(pc_get(point, 'm_roll') * 0.5) as t3,
                           cos(pc_get(point, 'm_pitch') * 0.5) as t4,
                           sin(pc_get(point, 'm_pitch') * 0.5) as t5,
                           st_transform(
                               st_setsrid(
                                   st_makepoint(pc_get(point, 'x'), pc_get(point, 'y')),
                                   4326),
                                %(srid)s) as xy,
                           pc_get(point, 'z') as z,
                           extract(epoch from
                                make_interval(weeks => (
                                    -- compute the GPS week number
                                    extract(days from
                                            timestamp %(filedate)s - gps.timestart) / 7)::int)
                                    -- find the beginning of GPS week
                                    + gps.timestart
                                    -- add the seconds
                                    + make_interval(secs => pc_get(point, 'm_time'))
                                ) as time
                            , paid
                    from (select
                            (row_number() over ())-1 as paid
                            , pc_explode(points) as point from {table_schema}.{table}) _
                    , (select timestamp '1980-01-06 00:00:00' timestart) as gps
                ),
                point as (
                    select pc_makepoint(%(pcid)s,
                                        ARRAY[
                                            t0 * t2 * t4 + t1 * t3 * t5,
                                            t0 * t3 * t4 - t1 * t2 * t5,
                                            t0 * t2 * t5 + t1 * t3 * t4,
                                            t1 * t2 * t4 - t0 * t3 * t5,
                                            st_x(xy), st_y(xy), z, time
                                        ]) as pt,
                           paid, param.time as time
                    from param
                )
                select paid as id, pc_patch(pt order by time)::pcpatch(%(pcid)s) as points
                from point group by paid
            '''
            # extract date from LANDINS_20170516_075157_PP
            filedate = payload['table'].split('_')[1]
            filedate = '{}-{}-{}'.format(filedate[0:4], filedate[4:6], filedate[6:8])
            parameters = {'pcid': pcid, 'srid': srid, 'filedate': filedate}
        else:
            select = '''
                select _id-1 as id, points from (
                    select row_number() over () as _id, points from {table_schema}.{table}
                ) _ order by id
            '''
            parameters = {}

        identifiers = map(sql.Identifier, (view_schema, view, table_schema, table))
        identifiers = zip(('view_schema', 'view', 'table_schema', 'table'), identifiers)
        identifiers = dict(identifiers)

        req = sql.SQL('''
            create materialized view {view_schema}.{view} as %s;
            create unique index on {view_schema}.{view} (id)
        ''' % select).format(**identifiers)
        Database.rowcount(req, parameters)

        if payload['sbet']:
            # create two indexes on pc_patchmin('time') and pc_patchmax('time'). This is
            # to make the time interpolation operation fast
            req = sql.SQL('''
                create index on {view_schema}.{view} (pc_patchmin(points, 'time'));
                create index on {view_schema}.{view} (pc_patchmax(points, 'time'))
            ''').format(**identifiers)
            Database.rowcount(req)

        req = views_sql + ' where v.schemaname = %(view_schema)s and v.matviewname = %(view)s'
        parameters = {'view_schema': view_schema, 'view': view}

        return Database.query_asjson(req, parameters), 201
