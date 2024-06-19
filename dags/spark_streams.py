import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import time


def create_spark_connection():
    s_conn = None

    # try:
    s_conn = SparkSession.builder \
        .appName('SparkDataStreaming') \
        .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.kafka:kafka-clients:3.7.0,org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.2.1,org.apache.commons:commons-pool2:2.8.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.1.2")\
        .getOrCreate()

    # s_conn.sparkContext.setLogLevel("ERROR")
    logging.info("Spark connection created successfully!")
    # except Exception as e:
    #     logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    # spark_df = None
    # try:
        # Create a spark dataframe storing the streaming data
    spark_df = spark_conn.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe', 'users_created').option('startingOffsets', 'earliest').load()
    print(spark_df)
    logging.info("kafka dataframe created successfully")
    # except Exception as e:
        # logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def foreach_batch_function(batch_df, batch_id):
    # Print the batch id
    print(f"Batch ID: {batch_id}")

    # Iterate over each row in the batch DataFrame
    for row in batch_df.collect():
        print(row)

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    cast_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print("Casting completed")
    query = cast_df.writeStream.format("console").start()
    query.awaitTermination()
    # time.sleep(10)
    # df.stop()

    # query.awaitTermination(500)
    # print(sel.show(5))

    return cast_df



def process():

    spark_conn = create_spark_connection()
    print("Spark Connection Type:", type(spark_conn))

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        # print("Spark DataFrame type: ", type(spark_df))
        create_selection_df_from_kafka(spark_df)
        # df.show()
        # query = selection_df.writeStream.format("console").start()
        # query.awaitTermination()

if __name__ == "__main__":
    process()


default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 6, 17, 11, 00)
}


with DAG('kafka-streams',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_kafka',
        python_callable=process
    )