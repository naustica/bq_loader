from google.cloud import bigquery
import os
import json


create_table(table_id: str,
             file_path: str,
             schema_file: str,
             source_format: str,
             ignore_unknown_values: bool):

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

    job_config = bigquery.LoadJobConfig(source_format=source_format,
                                        ignore_unknown_values=ignore_unknown_values,
                                        schema=schema)

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
