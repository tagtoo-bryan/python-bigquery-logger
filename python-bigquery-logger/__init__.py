import httplib2
from apiclient.discovery import build
import core
import logging
import datetime
from google_api_client import core
import time
import logging


BIGQUERY_SCOPE = 'https://www.googleapis.com/auth/bigquery'
BIGQUERY_SESSION = 'bigquery'

def get_service():
    global http, service
    if service is None:
        http = core.build_http(BIGQUERY_SCOPE, BIGQUERY_SESSION)
        service = build('bigquery', 'v2', http=http)
        return service
    else:
        return service


class BigQueryError(Exception):
    pass


class BigQueryClient(object):

    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def _make_request(self, method, body):
        """Make request to API endpoint

        Note: Ignoring SSL cert validation due to intermittent failures
        http://requests.readthedocs.org/en/latest/user/advanced/#ssl-cert-verification
        """
        service = get_service()
        tabledata = service.tabledata()
        result = tabledata.insertAll(
            projectId=self.project_id,
            datasetId=self.dataset_id,
            tableId=self.table_id,
            body=body
        ).execute()

        return result

    def insertall_message(self, text, **params):
        """tabledata().insertAll()

        This method insert a message into BigQuery

        Check docs for all available **params options:
        https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll
        """
        method = 'tabledata().insertAll()'

        if 'rows' in params:
            params['rows'][0].update({
                'json': {'text': text},
            })
        else:
            params['rows'] = [{
                'json': {'text': text},
            }]

        body = params
        return self._make_request(method, body)

class BigQueryHandler(logging.Handler):
    """A logging handler that posts messages to a BigQuery channel!

    References:
    http://docs.python.org/2/library/logging.html#handler-objects
    """
    def __init__(self, project_id, dataset_id, table_id, **kwargs):
        super(BigQueryHandler, self).__init__()
        self.client = BigQueryClient(project_id, dataset_id, table_id)
        self._kwargs = kwargs

    def emit(self, record):
        message = self.format(record)
        self.client.insertall_message(message, **self._kwargs)
