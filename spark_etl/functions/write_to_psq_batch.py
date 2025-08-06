from config import (
    SPARK_SINK_PSQL_HOST, 
    SPARK_SINK_PSQL_PORT, 
    SPARK_SINK_PSQL_DATABASE,
    SPARK_SINK_PSQL_SCHEME,
    SPARK_SINK_PSQL_USER,
    SPARK_SINK_PSQL_PASSWORD
    )

def write_to_postgres_dynamic(df, table_name):
    def write_batch(batch_df, batch_id):
        print(f"[DEBUG] START BATCH {batch_id}")
        batch_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://{host}:{port}/{database}".format(
                        host=SPARK_SINK_PSQL_HOST,
                        port=SPARK_SINK_PSQL_PORT,
                        database=SPARK_SINK_PSQL_DATABASE)) \
            .option("dbtable", f"{SPARK_SINK_PSQL_SCHEME}.{table_name}") \
            .option("user", SPARK_SINK_PSQL_USER) \
            .option("password", SPARK_SINK_PSQL_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .save()
        print(f"[DEBUG] END BATCH {batch_id}")
    return write_batch
