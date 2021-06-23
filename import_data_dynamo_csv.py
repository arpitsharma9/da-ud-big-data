import boto3
import json
import os
#bucket_name = os.environ['cand-c3-s3-app1-arpit']
csv_key = os.environ['csvdynamo.csv'] # csvdynamo.csv
table_name = os.environ['Order']

# temprorary file to store csv downloaded from s3
tmp_csv_file = '/tmp/' + csv_key
s3 = boto3.resource('s3')
db_table = boto3.resource('dynamodb').Table(table_name)
def save_to_dynamodb(id, name, co):
  return db_table.put_item(
      Item={
        'emp_id': int(id),
        'Name': name,
        'Company': co
      })
def lambda_handler(event, context):
    s3.meta.client.download_file(
                  bucket_name, 
                  csv_key, 
                  tmp_csv_file)
    with open(tmp_csv_file, 'r') as f:
      next(f) # skip header
      for line in f:
        id, name, co = line.rstrip().split(',')
        result = save_to_dynamodb(id, name, co)
        print(result)
    return {'statusCode': 200}