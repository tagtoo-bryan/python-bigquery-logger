#import httplib2
#from apiclient.discovery import build
#import core
import logging
import datetime
#from google_api_client import core
import time
import logging

class BigQueryError(Exception):
    pass


class BigQueryClient(object):

    def __init__(self, service, project_id):
        self.service = service
        self.project_id = project_id

    def _make_request(self, method, dataset_id, table_id, body):
        """Make request to API endpoint

        Note: Ignoring SSL cert validation due to intermittent failures
        http://requests.readthedocs.org/en/latest/user/advanced/#ssl-cert-verification
        """
        tabledata = self.service.tabledata()
        response = tabledata.insertAll(
            projectId=self.project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=body
        ).execute()

        return response

    def insertall_message(self, dataset_id, table_id, text):
        """tabledata().insertAll()

        This method insert a message into BigQuery

        Check docs for all available **params options:
        https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll
        """
        method = 'tabledata().insertAll().execute()'

        body = {}
        body['rows'] = [{
            'json': {'logging': text},
        }]

        body["kind"] = "bigquery#tableDataInsertAllRequest"
        return self._make_request(method, dataset_id, table_id, body)

class BigQueryHandler(logging.Handler):
    """A logging handler that posts messages to a BigQuery channel!

    References:
    http://docs.python.org/2/library/logging.html#handler-objects
    """
    def __init__(self, service, project_id):
        super(BigQueryHandler, self).__init__()
        self.client = BigQueryClient(service, project_id)

    def emit(self, dataset_id, table_id, record):
        try:
            # error when type(record) == str
            message = self.format(record)
        except:
            message = record

        return self.client.insertall_message(dataset_id, table_id, message)

