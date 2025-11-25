# DynamoDB Writer

A utility class for writing data to Amazon DynamoDB, designed to work with PySpark DataFrames.

## Installation

```bash
pip install ddb_writer
```

## Usage

```python
from ddb_writer import DynamoDBWriter

# Example usage
# df = ... # Your PySpark DataFrame
# write_capacity = 100 # 사용할 write capacity
# with DynamoDBWriter("your-table-name", write_capacity) as writer:
#     writer.write_dynamodb(df)
```
