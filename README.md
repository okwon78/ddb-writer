# DynamoDB Writer

A utility class for writing data to Amazon DynamoDB, designed to work with PySpark DataFrames.

## Prerequisites

This package requires `pyspark` to be installed in your environment. It does not install `pyspark` automatically to allow using the cluster's provided version.

## Installation

### From GitHub

```bash
pip install git+https://github.com/okwon78/ddb-writer.git
```

### From Source (Development)

```bash
git clone https://github.com/okwon78/ddb-writer.git
cd ddb-writer
pip install -e .
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
