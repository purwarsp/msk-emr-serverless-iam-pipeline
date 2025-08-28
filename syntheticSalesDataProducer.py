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
#  syntheticSalesDataProducer.py is random data generator scripts created for Blog demo.
#  Authored by Nitin Kumar and Shubham Purwar
#
#  python3 syntheticSalesDataProducer.py --num_records 1000 --sales_data_topic sales_data_topic --bootstrap_server localhost:9092 --region us-east-1
###
import pandas as pd
import random
import string
import time
import socket
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import argparse
import sys

class MSKTokenProvider():
    def __init__(self, region):
        self.region = region

    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region=self.region)
        return token

# Define product categories and corresponding product names
product_categories = {
    'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smart TV'],
    'Clothing': ['T-shirt', 'Jeans', 'Dress', 'Shirt', 'Jacket'],   
    'Beauty': ['Perfume', 'Makeup', 'Skincare', 'Hair Products', 'Bath Products']
}

# Function to generate a random transaction ID
def generate_transaction_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=15))

# Function to generate a random customer ID
def generate_customer_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

# Function to generate a random gender
def generate_gender():
    return random.choice(['Male', 'Female'])

# Function to generate a random age
def generate_age():
    return random.randint(18, 50)

# Function to generate a random product category and product name
def generate_product_category_and_name():
    category = random.choice(list(product_categories.keys()))
    product_name = random.choice(product_categories[category])
    return category, product_name

# Function to generate a random quantity
def generate_quantity():
    return random.randint(1, 5)

# Function to generate a random price per unit
def generate_price_per_unit():
    return round(random.uniform(10, 500), 2)

# Function to generate a random COGS (Cost of Goods Sold)
def generate_cogs(price_per_unit):
    return round(price_per_unit * random.uniform(0.5, 0.9), 2)

# Function to generate a random date within the last month
def generate_date():
    start_date = datetime.now() - timedelta(days=31)
    end_date = datetime.now() - timedelta(days=1)
    return start_date + (end_date - start_date) * random.random()

# Function to generate synthetic sales data
def generate_synthetic_sales_data(num_records):
    data = []
    for _ in range(num_records):
        transaction_id = generate_transaction_id()
        date = generate_date()
        customer_id = generate_customer_id()
        gender = generate_gender()
        age = generate_age()
        product_category, product_name = generate_product_category_and_name()
        quantity = generate_quantity()
        price_per_unit = generate_price_per_unit()
        cogs = generate_cogs(price_per_unit)
        total_amount = quantity * price_per_unit
        data.append([transaction_id, date, customer_id, gender, age, product_category, product_name, quantity, price_per_unit, cogs, total_amount])
    return data

# Function to generate synthetic sales data for the current date
def generate_synthetic_sales_data_current_date(num_records):
    data = []
    current_date = datetime.now()
    for _ in range(num_records):
        transaction_id = generate_transaction_id()
        customer_id = generate_customer_id()
        gender = generate_gender()
        age = generate_age()
        product_category, product_name = generate_product_category_and_name()
        quantity = generate_quantity()
        price_per_unit = generate_price_per_unit()
        cogs = generate_cogs(price_per_unit)
        total_amount = quantity * price_per_unit
        data.append([transaction_id, current_date, customer_id, gender, age, product_category, product_name, quantity, price_per_unit, cogs, total_amount])
    return data

# Main function
def main(num_records, bootstrap_server, sales_data_topic, region):

    global tp
    tp = MSKTokenProvider(region)
    #column names in data
    columns = ['transaction_id', 'date', 'customer_id', 'gender', 'age', 'product_category', 'product_name', 'quantity', 'price_per_unit', 'cogs', 'total_amount']

    data = generate_synthetic_sales_data(num_records)
    df = pd.DataFrame(data, columns=columns)

    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Kafka producer configuration
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server],
                             security_protocol='SASL_SSL',
                             sasl_mechanism='OAUTHBEARER',
                             sasl_oauth_token_provider=tp,
                             client_id=socket.gethostname(),
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    print("Start producing records to topic - backfill last 30 days sales data running")
    # Produce DataFrame records to Kafka (backfill last 30 days sales data)
    for _, row in df.iterrows():
        try:
            record = row.to_dict()
            print(record)
            producer.send(sales_data_topic, value=record)
            producer.flush()
            print("Produced!")
        except Exception:
            print("Failed to send message.")
    
    print("Start producing records to topic - 3 realtime events every 10 seconds")
    print("=====================================================================")
    while True:
        real_data = generate_synthetic_sales_data_current_date(3)
        df_realtime = pd.DataFrame(real_data, columns=columns)
        
        for column in df_realtime.columns:
            if pd.api.types.is_datetime64_any_dtype(df_realtime[column]):
                df_realtime[column] = df_realtime[column].dt.strftime('%Y-%m-%d %H:%M:%S')


        try:
            for _, row in df_realtime.iterrows():
                record = row.to_dict()
                print(record)
                producer.send(sales_data_topic, value=record)
                producer.flush()
                print("Produced!")
        except Exception:
            print("Failed to send message.")
            producer.close()

        time.sleep(10)


# Call the main function
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process command-line arguments")
    parser.add_argument('--num_records', required=False,type=int,default=1000, help="Number of backfill records to generate for last 30 days (default value 1000)")
    parser.add_argument('--bootstrap_server',required=True, type=str,help="MSK bootstrap server string, Ex: boot-xxxxx.c1.kafka-serverless.us-east-1.amazonaws.com:9098")
    parser.add_argument('--sales_data_topic',required=False, type=str,default="sales-order-blog",help="topic name to insert the records, default value as sales-order-blog")
    parser.add_argument('--region', required=False, type=str, default='us-east-1', help="AWS region for MSK (default: us-east-1)")
    args, _ = parser.parse_known_args()
    num_records = args.num_records
    bootstrap_server = args.bootstrap_server
    sales_data_topic = args.sales_data_topic
    region = args.region
    main(num_records, bootstrap_server, sales_data_topic, region)