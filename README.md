# AWS Serverless Streaming Pipeline with MSK, EMR, and IAM Authentication

This repository contains the CloudFormation template and sample code to deploy a **secure, serverless streaming pipeline** on AWS. The solution demonstrates how to ingest, process, and analyze streaming data using **Amazon MSK Serverless** (for Kafka-based data ingestion), **Amazon EMR Serverless** (for Spark-based stream processing), and **AWS IAM** for authentication—eliminating the need for complex certificate and key management.

**For more detailed steps, please checkout the blog** 

[Build a secure serverless streaming pipeline with Amazon MSK Serverless, Amazon EMR Serverless and IAM](https://aws.amazon.com/blogs/big-data/build-a-secure-serverless-streaming-pipeline-with-amazon-msk-serverless-amazon-emr-serverless-and-iam/)

## Architecture Overview

![Architecture](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/26/blog-4406-image-1.png)

The workflow consists of the following steps:

   1. The architecture begins with an MSK Serverless cluster set up with IAM authentication. An Amazon Elastic Compute Cloud (Amazon EC2) instance runs a Python script producer.py that acts as a data producer, sending sample data to a Kafka topic within the cluster.
   2. The Spark Streaming job retrieves data from the Kafka topic, stores it in Amazon Simple Storage Service (Amazon S3), and creates a corresponding table in the AWS Glue Data Catalog. As it continuously consumes data from the Kafka topic, the job stays up-to-date with the latest streaming data. With checkpointing enabled, the job tracks processed records, allowing it to resume from where it left off in case of a failure, providing seamless data processing.
   3. To analyze this data, users can use Athena, a serverless query service. Athena enables interactive SQL-based exploration of data directly in Amazon S3 without the need for complex infrastructure management.

## Key Features

- **Serverless Architecture:** No cluster management for MSK or EMR.
- **IAM Authentication:** Secure access to Kafka without certificates or keystores.
- **Integration:** Seamless ingestion, processing, and querying using AWS Glue Data Catalog and Athena.
- **Scalability:** Handles large-scale streaming workloads with ease.
- **Reproducibility:** Infrastructure as code via CloudFormation.
