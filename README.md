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
df = spark.read.parquet("/Users/obiwan/Dev/DDB/data.parquet") # Your PySpark DataFrame
write_capacity = 1000 # write capacity to be used
with DynamoDBWriter("dynamodb-table-name", write_capacity) as writer:
    writer.write_dynamodb(df)
```
