import logging
import base64
import StringIO
import json
import csv
from redash.query_runner import BaseSQLQueryRunner, register

logger = logging.getLogger(__name__)

try:
    from oauth2client.client import SignedJwtAssertionCredentials
    from apiclient.discovery import build
    import httplib2
    enabled = True
except ImportError as e:
    logger.info(str(e))
    enabled = False


class DOQL(BaseSQLQueryRunner):
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'host': {
                    'type': 'string',
                    'default': '127.0.0.1'
                },
                'user': {
                    'type': 'string'
                },
                'passwd': {
                    'type': 'string',
                    'title': 'Password'
                }
            },
            'required': ['host'],
            'secret': ['passwd']
        }

    @classmethod
    def type(cls):
        return "doql"

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def name(cls):
        return "DOQL"

    def _get_tables(self, schema):
        query = "SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog', 'information_schema');"

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        results = json.loads(results)

        for row in results['rows']:
            table_name = row['table_name']

            if table_name not in schema:
                schema[table_name] = {'name': table_name, 'columns': []}

            schema[table_name]['columns'].append(row['column_name'])

        return schema.values()

    def run_query(self, query, user):
        logger.debug("DOQL running query: %s", query)
        error = None
        host = self.configuration.get('host', '')
        username = self.configuration.get('user', '')
        password = self.configuration.get('passwd', '')

        try:
            query = query.split('*/')[1]
        except IndexError:
            pass

        h = httplib2.Http(".cache", disable_ssl_certificate_validation=True)
        h.follow_all_redirects = True
        resp, content = h.request("https://%s/services/data/v1.0/query/?query=%s&header=yes" % (host, query),
                                  "GET",
                                  headers={
                                      'Content-Type': 'application/json',
                                      'Authorization': 'Basic %s' % base64.encodestring(username + ':' + password)
                                  })

        data = []
        f = StringIO.StringIO(content.encode('utf-8'))
        for row in csv.DictReader(f, delimiter=','):
            data.append(row)

        # for nope
        if query == DOQL.noop_query and int(data[0]['?column?']) == 1:
            return {}, None

        try:
            columns = [{"type": None, "friendly_name": x, "name": x} for x in data[0]]
        except IndexError:
            columns = None
            error = "No results.Please check query."

        return json.dumps({'rows': data, 'columns': columns}), error

register(DOQL)
