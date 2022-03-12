# bq_loader

## Usage

```python
from bq_loader import create_table

create_table(table_id='subugoe-collaborative.resource.cr_snapshot',
             file_path='/scratch/users/haupka/transform',
             schema_file='schema_crossref.json',
             source_format='jsonl',
             ignore_unknown_values=True)
```
