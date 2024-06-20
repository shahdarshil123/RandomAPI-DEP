import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    logging.info("Table created successfully!")

def insert_data(session, row):
    logging.info("Inserting data...")

    # user_id = row['id']
    first_name = row['first_name']
    last_name = row['last_name']
    gender = row['gender']
    address = row['address']
    post_code = row['post_code']
    email = row['email']
    username = row['username']
    registered_date = row['registered_date']
    phone = row['phone']
    picture = row['picture']

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users (id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (uuid.uuid4(), first_name, last_name, gender, address,
              post_code, email, username, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
    return spark_df

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        # StructField("id", StringType(), False),
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

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    logging.info(f"Selection DataFrame: {sel}")

    return sel

def process_batch(df, epoch_id, session):
    rows = df.collect()
    for row in rows:
        insert_data(session, row.asDict())

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session is not None:
                create_keyspace(session)
                create_table(session)
                logging.info("Streaming is being started...")

                try:
                    streaming_query = (selection_df.writeStream
                                       .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, session))
                                       .outputMode("append")
                                       .start())
                    streaming_query.awaitTermination()
                except Exception as e:
                    logging.error(f"Error starting streaming query: {e}")
