import logging

class BigQueryError(Exception):
    pass


class BigQueryClient(object):

    def __init__(self, service, project_id, dataset_id, table_id):
        self.service = service
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def _make_request(self, method, body):
        """Make request to API endpoint
        """
        tabledata = self.service.tabledata()
        response = tabledata.insertAll(
            projectId=self.project_id,
            datasetId=self.dataset_id,
            tableId=self.table_id,
            body=body
        ).execute()

        return response

    def insertall(self, rows):
        """
        This method insert rows into BigQuery
        """
        method = 'tabledata().insertAll().execute()'

        body = {}
        body['rows'] = [{'json': row} for row in rows]
        body["kind"] = "bigquery#tableDataInsertAllRequest"
        return self._make_request(method, body)

    def insertall_message(self, text):
        """tabledata().insertAll()

        This method insert a message into BigQuery

        Check docs for all available **params options:
        https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll
        """
        return self.insertall([{'logging': text}])


class BigQueryHandler(logging.BufferingHandler):
    """A logging handler that posts messages to a BigQuery channel!

    References:
    http://docs.python.org/2/library/logging.html#handler-objects
    """

    def __init__(self, service, project_id, dataset_id, table_id, capacity=200):
        super(BigQueryHandler, self).__init__(capacity)
        self.client = BigQueryClient(service, project_id, dataset_id, table_id)


    def flush(self):
        """
        Override to implement custom flushing behaviour.

        This version just zaps the buffer to empty.
        """
        self.acquire()
        try:
            self.client.insertall(k.__dict__ for k in self.buffer)
            self.buffer = []
        finally:
            self.release()
