# bq_loader

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
