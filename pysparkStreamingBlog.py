# This Python file uses the following encoding: utf-8
'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''
### 
#  pysparkStreamingBlog.py is pyspark script (act as kafka consumer) to consume topic records...created for Blog demo.
#  Authored by Nitin Kumar and Shubham Purwar
#
#  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,software.amazon.msk:aws-msk-iam-auth:2.2.0 pysparkStreamingBlog.py \
#    --topic_input "sales_data_topic" \
#    --kafka_bootstrap_servers "boot-xxxxx.c2.kafka-serverless.us-east-1.amazonaws.com:9098" \
#    --output_s3_path "s3://xxxxxx/blog/topic/sales-order-data/" \
#    --checkpointLocation "s3://xxxxxxxx/blog/topic/checkpoint-sales-order-data/" \
#    --database_name "emrblog" \
#    --table_name "sales_order_data" 
###
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse

def write_to_s3_sinks(dataframe: DataFrame, _):
    dataframe.write.mode("append").format("parquet").option("path", output_s3_path).saveAsTable(f"{database_name}.{table_name}")

def process_data(spark, topic_input, kafka_bootstrap_servers, checkpointLocation):
    topic_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topic_input)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol","SASL_SSL")
    .option("kafka.sasl.mechanism","AWS_MSK_IAM")
    .option("kafka.sasl.jaas.config","software.amazon.msk.auth.iam.IAMLoginModule required;")
    .option("kafka.sasl.client.callback.handler.class","software.amazon.msk.auth.iam.IAMClientCallbackHandler")
    .load()
    .selectExpr("CAST(value AS STRING)")
    )

    #topic_df.printSchema()
    sales_data_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("date", TimestampType(), True),
            StructField("customer_id", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price_per_unit", DoubleType(), True),
            StructField("cogs", DoubleType(), True),
            StructField("total_amount", DoubleType(), True)
        ])

    streaming_df = topic_df.withColumn("values", from_json(col("value"),sales_data_schema)).selectExpr("values.*")
    streaming_df.printSchema()
    streaming_df.writeStream.foreachBatch(write_to_s3_sinks).option("checkpointLocation", checkpointLocation).start().awaitTermination()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process sales order data from MSK topic")
    parser.add_argument("--topic_input", type=str, required=True, help="Kafka topic to read data from")
    parser.add_argument("--kafka_bootstrap_servers", type=str, required=True, help="Kafka bootstrap servers string")
    parser.add_argument("--output_s3_path", type=str, required=True, help="S3 path to write processed data")
    parser.add_argument("--checkpointLocation", type=str, required=True, help="S3 path for streaming checkpoint location")
    parser.add_argument("--database_name", type=str, required=True, help="Glue catalog database name")
    parser.add_argument("--table_name", type=str, required=True, help="Glue catalog table name")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("msk-spark-emrserverless-blog").enableHiveSupport().getOrCreate()
    # create database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.database_name}")
    spark.sql("show databases")
    output_s3_path = args.output_s3_path
    database_name = args.database_name
    table_name = args.table_name
    process_data(spark, args.topic_input, args.kafka_bootstrap_servers, args.checkpointLocation)