# bq_loader - Interactive Command Line Interface for Google BigQuery

[![PyPI - Downloads](https://img.shields.io/pypi/dm/bq_loader)](https://pypi.org/project/bq_loader/)
[![License](https://img.shields.io/github/license/naustica/bq_loader)](https://github.com/naustica/bq_loader/blob/master/LICENSE.txt)
[![PyPI - Version](https://img.shields.io/pypi/v/bq_loader)](https://pypi.org/project/bq_loader/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bq_loader)](https://pypi.org/project/bq_loader/)

## Install

```bash
pip install bq_loader
```

## Usage

### Command Line Interface

```bash
bqloader
```

![Demo](https://raw.githubusercontent.com/naustica/bq_loader/master/media/demo.gif)

### API

#### Create a table from a local file or directory

```python
from bq_loader import create_table_from_local

 create_table_from_local(table_id='snapshot',
                         project_id='subugoe-collaborative',
                         dataset_id='resources',
                         file_path='test_data/',
                         schema_file_path='test_schema/schema_crossref.json',
                         source_format='jsonl',
                         write_disposition='WRITE_APPEND',
                         table_description='Test Table generated by bq_loader',
                         ignore_unknown_values=True)
```

#### Create a table from a Google Bucket

```python
from bq_loader import create_table_from_bucket

create_table_from_bucket(uri='gs://bigschol/tests/*',
                         table_id='bq_loader_test',
                         project_id='subugoe-collaborative',
                         dataset_id='resources',
                         schema_file_path='test_schema/schema_crossref.json',
                         source_format='jsonl',
                         write_disposition='WRITE_EMPTY',
                         table_description='Test Table generated by bq_loader',
                         ignore_unknown_values=True)
```

#### Upload local files to a Google Bucket

```python
from bq_loader import upload_files_to_bucket

upload_files_to_bucket(bucket_name='bigschol',
                       file_path='test_data/',
                       gcb_dir='tests')
```
