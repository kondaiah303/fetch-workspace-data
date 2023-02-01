from boto3.dynamodb.conditions import Attr
import json
from collections import OrderedDict
from decimal import *
from db_tables import *


class DecimalEncoder(json.JSONEncoder):

    def default(self, obj):
        """
            changing decimal to integer
        """
        # if passed in object is instance of Decimal
        # convert it to a string
        if isinstance(obj, Decimal):
            # if float(obj) % 1 == 0:
            #     return int(obj)
            return int(obj)
        return json.JSONEncoder.default(self, obj)


def workspace_id_with_maximum_studies_published_data():
    """
        printing workspace_id which published maximum studies
    """

    print('Retrieving the data')
    res = DynamodbTables().study_details_table.scan(
        FilterExpression=Attr('status').eq('ACTIVE') | Attr('status').eq('CLOSED')
    )
    workspace_id_list1 = []
    while "LastEvaluatedKey" in res:
        key = res['LastEvaluatedKey']
        res = DynamodbTables().study_details_table.scan(
            FilterExpression=Attr('status').eq('ACTIVE') | Attr('status').eq('CLOSED'), ExclusiveStartKey=key
        )
        for item in res['Items']:
            workspace_id_list1.append(item.get('workspace_id'))
    workspace_id_list2 = list(OrderedDict.fromkeys(workspace_id_list1))
    workspace_id_count = {workspace_id: workspace_id_list1.count(workspace_id) for workspace_id in workspace_id_list2}
    count = 0
    workspace_id = ''
    for key, value in workspace_id_count.items():
        if count < value:
            count = value
            workspace_id = key
    print('maximum studies published = ', {workspace_id: count})


def study_with_maximum_tester_responses():
    """
        printing study_id which having maximum tester responses
    """

    res = DynamodbTables().tester_details_table.scan(
        FilterExpression=Attr('test_status').eq('FINISHED')
    )
    study_id_list1 = []
    while "LastEvaluatedKey" in res:
        key = res['LastEvaluatedKey']
        res = DynamodbTables().tester_details_table.scan(
            FilterExpression=Attr('test_status').eq('FINISHED'), ExclusiveStartKey=key
        )
        for item in res['Items']:
            study_id_list1.append(item.get('study_id'))
    study_id_list2 = list(OrderedDict.fromkeys(study_id_list1))
    study_id_count = {study_id: study_id_list1.count(study_id) for study_id in study_id_list2}
    count = 0
    study_id = ''
    for key, value in study_id_count.items():
        if count < value:
            count = value
            study_id = key
    print('study with maximum tester responses = ', {study_id: count})


def more_tester_count_for_most_active_workspace_id_data():
    res1 = DynamodbTables().study_details_table.scan()
    study_details_data_list1 = []
    workspace_id_and_tester_count_list = []
    while 'LastEvaluatedKey' in res1:
        key = res1['LastEvaluatedKey']
        res1 = DynamodbTables().study_details_table.scan(ExclusiveStartKey=key)
        items = res1['Items']
        for item in items:
            if item.get('tester_counts') is not None:
                study_details_data_list1.append(item)

    for item in study_details_data_list1:
        if item.get('tester_counts').get('active') is None:
            item['tester_counts']['active'] = 0
        workspace_id_and_tester_count_list.append({item.get('workspace_id'): int(item.get('tester_counts').get('active'))})
    result = {}
    for data in workspace_id_and_tester_count_list:
        for key in data.keys():
            result[key] = result.get(key, 0) + data[key]
    return result


def more_studies_created():
    res2 = DynamodbTables().study_details_table.scan(
        FilterExpression=Attr('status').eq('ACTIVE') | Attr('status').eq('CLOSED') | Attr('status').eq('DRAFT')
    )
    workspace_id_list = []
    study_details_data_list2 = []
    while "LastEvaluatedKey" in res2:
        key = res2['LastEvaluatedKey']
        res2 = DynamodbTables().study_details_table.scan(
            FilterExpression=Attr('status').eq('ACTIVE') | Attr('status').eq('CLOSED') | Attr('status').eq('DRAFT'),
            ExclusiveStartKey=key)
        items = res2['Items']
        for item in items:
            study_details_data_list2.append(item)
            workspace_id_list.append(item.get('workspace_id'))

    study_id_count_dict = {study_id: workspace_id_list.count(study_id) for study_id in workspace_id_list}
    return study_id_count_dict


def most_active_workspace_id():
    """
        printing the most active workspace_id
    """
    more_tester_count_data = more_tester_count_for_most_active_workspace_id_data()
    more_studies_data = more_studies_created()
    final_study_id_count_list = []
    for key1, value1 in more_tester_count_data.items():
        for key2, value2 in more_studies_data.items():
            if key1 == key2:
                value = value1+value2
                more_studies_data.update({key1: value})
                final_study_id_count_list.append({key1: value})
            if key2 not in more_tester_count_data:
                if {key2: value2} not in final_study_id_count_list:
                    final_study_id_count_list.append({key2: value2})
        if key1 not in more_studies_data:
            if {key1: value1} not in final_study_id_count_list:
                final_study_id_count_list.append({key1: value1})
    count = 0
    workspace_id = ''
    for data in final_study_id_count_list:
        for key, value in data.items():
            if value > count:
                count = value
                workspace_id = key
    print({workspace_id: count})


def main():
    workspace_id_with_maximum_studies_published_data()
    study_with_maximum_tester_responses()
    most_active_workspace_id()


if __name__ == "__main__":
    main()
