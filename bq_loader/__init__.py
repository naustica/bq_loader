from google.cloud import bigquery
import os
import json


def create_table(table_id: str,
                 file_path: str,
                 schema_file: str,
                 source_format: str,
                 write_disposition: str,
                 destination_table_description: str,
                 ignore_unknown_values: bool) -> None:


    with open(schema_file, 'r') as schema_file:
        schema = json.load(schema_file)

    if source_format == 'jsonl':
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    elif source_format == 'avro':
        source_format = bigquery.SourceFormat.AVRO

    elif source_format == 'csv':
        source_format = bigquery.SourceFormat.CSV

    elif source_format == 'mro':
        source_format = bigquery.SourceFormat.mro

    elif source_format == 'orc':
        source_format = bigquery.SourceFormat.ORC

    elif source_format == 'parquet':
        source_format = bigquery.SourceFormat.PARQUET

    else:
        raise ValueError('Source format {0} is not implemented.'.format(source_format))

    if write_disposition not in ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']:
        raise ValueError('Type of class WriteDisposition {0} is not implemented.'.format(source_format))

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(source_format=source_format,
                                        ignore_unknown_values=ignore_unknown_values,
                                        schema=schema,
                                        write_disposition=write_disposition,
                                        destination_table_description=destination_table_description)

    jobs = []

    for root, dirs, files in os.walk(os.path.abspath(file_path)):
        for file in files:
            with open(os.path.join(root, file), 'rb') as source_file:
                job = client.load_table_from_file(source_file,
                                                  table_id,
                                                  job_config=job_config)

                jobs.append(job)

    for job in jobs:
        job.result()
