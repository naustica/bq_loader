# bq_loader

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

### API

```python
from bq_loader import create_table

create_table(table_id='subugoe-collaborative.resources.cr_snapshot',
             file_path='/scratch/users/haupka/transform',
             schema_file='schema_crossref.json',
             source_format='jsonl',
             write_disposition='WRITE_EMPTY',
             destination_table_description='Test Table',
             ignore_unknown_values=True)
```
