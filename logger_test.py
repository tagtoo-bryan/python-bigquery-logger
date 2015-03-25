import unittest
from mock import *
from py_bigquery_logger import BigQueryClient, BigQueryHandler

class TestBigqueryLogger(unittest.TestCase):

    def setUp(self):
        self.mock_service = Mock()
        self.mock_tabledata = Mock()
        self.mock_service.tabledata.return_value = self.mock_tabledata

        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.table_id = 'table_id'
        self.record = 'logging record'
        self.body = {
            "kind": "bigquery#tableDataInsertAllRequest",
            "rows": [{
                "json": {'logging': self.record}
            }]
        }

    def tearDown(self):
        pass

    def test_bigquerh_insertall(self):
        self.mock_tabledata.insertAll.return_value.execute.return_value = {
            "kind": "my#tableDataInsertAllResponse",
        }

        import logging
        record = logging.LogRecord(
            name = "test-name",
            level = logging.DEBUG,
            pathname = 'test-path-name',
            lineno = 1,
            msg = 'test-msg',
            args = {},
            exc_info = None
        )

        handler = BigQueryHandler(
            self.mock_service,
            self.project_id,
            self.dataset_id,
            self.table_id
        )
        handler.handle(record)
        response = handler.flush()

        self.assertEqual(response, {"kind": "my#tableDataInsertAllResponse"})
        self.mock_service.tabledata.assert_called_with()
        self.mock_tabledata.insertAll.assert_called_with(
            projectId = self.project_id,
            datasetId = self.dataset_id,
            tableId = self.table_id,
            body = self.body
        )

if __name__ == '__main__':
    unittest.main()

