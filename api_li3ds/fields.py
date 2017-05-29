import datetime
from flask_restplus import fields


class DateTime(fields.DateTime):

    def format_iso8601(self, dt):
        dt_utc = dt.replace(tzinfo=datetime.timezone.utc) - dt.utcoffset()
        return dt_utc.isoformat()
