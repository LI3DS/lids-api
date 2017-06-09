import datetime
from flask_restplus import fields


class DateTime(fields.DateTime):

    def format_iso8601(self, dt):
        dt_utc = dt.replace(tzinfo=datetime.timezone.utc) - dt.utcoffset()
        return dt_utc.isoformat()


class Json(fields.Raw):

    def __init__(self, as_list=False, **kwargs):
        super().__init__(**kwargs)
        self.as_list = as_list

    def schema(self):
        schema = super().schema()
        if self.as_list:
            schema['type'] = 'array'
        return schema
