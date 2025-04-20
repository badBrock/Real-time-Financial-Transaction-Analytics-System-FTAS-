from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, DoubleType, LongType, StringType, StructType

# Use internal Docker network names instead of localhost
KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = 'financial_transactions'
AGGREGATES_TOPIC = 'transaction_aggregates'
ANOMALIES_TOPIC = 'transaction_anomalies'
CHECKPOINT_DIR = '/mnt/spark-checkpoints'
STATES_DIR = '/mnt/spark-state'

spark = (SparkSession.builder
    .appName('FinancialTransactionsProcessor')
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
    .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
    .config('spark.sql.streaming.stateStore.stateStoreDir', STATES_DIR)
    .config('spark.sql.shuffle.partitions', '20')
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

transaction_schema = StructType([
    StructField('transactionId', StringType(), nullable=True),
    StructField('userId', StringType(), nullable=True),
    StructField('merchantId', StringType(), nullable=True),
    StructField('amount', DoubleType(), nullable=True),
    StructField('transactionTime', LongType(), nullable=True),
    StructField('transactionType', StringType(), nullable=True),
    StructField('location', StringType(), nullable=True),
    StructField('paymentMethod', StringType(), nullable=True),
    StructField('isInternational', StringType(), nullable=True),
    StructField('currency', StringType(), nullable=True),
])

kafka_stream = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .load())

transactions_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), transaction_schema).alias("data")) \
    .select("data.*")

transactions_df = transactions_df.withColumn("transactionTimestamp",
                                             (col('transactionTime') / 1000).cast("timestamp"))


aggregated_df = transactions_df.groupBy("merchantId") \
    .agg(
        sum("amount").alias("totalAmount"),
        count("*").alias("transactionCount")
    )
aggregation_query = aggregated_df \
    .withColumn("key", col("merchantId").cast("string")) \
    .withColumn("value", to_json(struct(
        col("merchantId"),
        col("totalAmount"),
        col("transactionCount")
    ))) \
    .selectExpr("key", "value") \
    .writeStream \
    .format('kafka') \
    .outputMode('update') \
    .option('kafka.bootstrap.servers', KAFKA_BROKERS) \
    .option('topic', AGGREGATES_TOPIC) \
    .option('checkpointLocation', f'{CHECKPOINT_DIR}/aggregates') \
    .start().awaitTermination()