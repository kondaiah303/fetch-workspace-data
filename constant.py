import boto3
dynamoDB = boto3.resource('dynamodb', region_name='ap-south-1')


class DynamodbTables:
    study_details_table = dynamoDB.Table('decode-cx-study-details')
    tester_details_table = dynamoDB.Table('decode-cx-tester-details')

