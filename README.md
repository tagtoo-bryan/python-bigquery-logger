python-bigquery-logger
==========

streaming the logger info to bigquery

## Usage

Before using, set your google api client well, ex: client secrets, in order to import core

Post a message into your BigQuery table specified with project ID, dataset ID, table ID

    >>> from py_bigquery_logger import BigQueryClient
    >>> from apiclient.discovery import build # google package
    >>> from google_api_client import core # google package
    >>> http = core.build_http('https://www.googleapis.com/auth/bigquery', 'bigquery')
    >>> service = build('bigquery', 'v2', http=http) 
    >>> client = BigQueryClient(service, 'project ID', 'dataset ID', 'table ID')
    >>> client.insertall_message("testing, testing...")
    { "kind": "bigquery#tableDataInsertAllResponse", "insertErrors": [] }


Integrate a BigQueryHandler into your logging!

    >>> import logging
    >>> from py_bigquery_logger import BigQueryHandler
    >>> from apiclient.discovery import build # google package
    >>> from google_api_client import core # google package
    >>> http = core.build_http('https://www.googleapis.com/auth/bigquery', 'bigquery')
    >>> service = build('bigquery', 'v2', http=http)
    
    >>> logger = logging.getLogger('test')
    >>> logger.setLevel(logging.DEBUG)
    
    >>> handler = BigQueryHandler(service, 'project ID', 'dataset ID', 'table ID')
    >>> handler.setLevel(logging.WARNING)
    >>> formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s (%(process)d): %(message)s')
    >>> handler.setFormatter(formatter)
    >>> logger.addHandler(handler)
    
    >>> logger.error("Oh noh!") # Will post the formatted message to the specified table


