


from ddb_writer import DynamoDBWriter

def main():

    export_df = spark.read.parquet("/Users/obiwan/Dev/DDB/data.parquet")
    with DynamoDBWriter(table=ddb_table, write_capacity=ddb_write_capacity) as client:
        client.write_dynamodb(
            dataframe=export_df.repartition(ddb_partitions),
        )

if __name__ == "__main__":
    main()