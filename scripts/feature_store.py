import pandas as pd
import numpy as np
import time
import boto3
from sagemaker.session import Session
from sagemaker import get_execution_role
from sagemaker.feature_store.feature_group import FeatureGroup

s3_client = boto3.client('s3')
sagemaker_session = Session()

def get_s3_objects(input_bucket_name, input_prefix):
    objects = s3_client.list_objects(Bucket=input_bucket_name, Prefix=input_prefix)
    keys = [oc['Key'] for oc in objects['Contents']]
    return(keys)

def delete_all_keys_in_bucket(bucket, prefix):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    to_delete = dict(Objects=[])
    for item in pages.search('Contents'):
        to_delete['Objects'].append(dict(Key=item['Key']))
        # flush once aws limit reached
        if len(to_delete['Objects']) >= 1000:
            s3_client.delete_objects(Bucket=bucket, Delete=to_delete)
            to_delete = dict(Objects=[])
    # flush rest
    if len(to_delete['Objects']):
        s3_client.delete_objects(Bucket=bucket, Delete=to_delete)

def read_tables(input_bucket_name, input_prefix):
    keys = get_s3_objects(input_bucket_name, input_prefix)
    convo_df = pd.read_csv('s3://{}/{}'.format(input_bucket_name, keys[0]))
    return(convo_df)

def define_type(to_feature_store, types_definitions):
    columns = to_feature_store.columns
    for c in columns:
        c_dtype = to_feature_store[c].dtype.str
        _expected_dtype = [k for k,v in types_definitions.items() if c in v][0]
        if c_dtype == _expected_dtype:
            continue
        else:
            if _expected_dtype != 'float':
                to_feature_store[c] = to_feature_store[c].astype(_expected_dtype)
                print(c, _expected_dtype)
            else:
                to_feature_store[c] = to_feature_store[c].values.astype(np.int64)
    to_feature_store_types = to_feature_store
    return(to_feature_store_types)

def delete_feature_group_if_exists(feature_group_name):
    sagemaker_client = boto3.client('sagemaker')
    response = sagemaker_client.list_feature_groups(NameContains=feature_group_name, FeatureGroupStatusEquals='Created')
    feature_group_summary = response['FeatureGroupSummaries'][0]
    is_created = feature_group_summary['FeatureGroupStatus'] == 'Created'
    if len(feature_group_summary) > 0 and is_created:
        sagemaker_client.delete_feature_group(FeatureGroupName=feature_group_name)

def check_feature_group_status(feature_group):
    status = feature_group.describe().get("FeatureGroupStatus")
    while status == "Creating":
        print("Waiting for Feature Group to be Created")
        time.sleep(10)
        status = feature_group.describe().get("FeatureGroupStatus")
    print(f"FeatureGroup {feature_group.name} successfully created.")
    

def executable(feature_group_name,
         identifier,
         sagemaker_role_name,
         input_bucket_name,
         input_prefix,
         type_definitions,
         feature_store_bucket,
         feature_store_prefix,
         event_time):
    sagemaker_session = Session()
    try:
        sagemaker_role = get_execution_role()
    except ValueError:
        iam = boto3.client('iam')
        sagemaker_role = iam.get_role(sagemaker_role_name)['Role']['Arn']
    delete_feature_group_if_exists(feature_group_name)
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=sagemaker_session)
    record_identifier_feature_name = identifier
    to_feature_store = read_tables(input_bucket_name,input_prefix)
    to_feature_store = to_feature_store.head(1000)
    to_feature_store = define_type(to_feature_store, type_definitions)
    feature_group.load_feature_definitions(data_frame=to_feature_store)
    feature_group.create(
        s3_uri='s3://{}/{}'.format(feature_store_bucket, feature_store_prefix),
        record_identifier_name=record_identifier_feature_name,
        event_time_feature_name=event_time,
        role_arn=sagemaker_role,
        enable_online_store=False)
    print('feature_group_create has been executed')
    check_feature_group_status(feature_group)
    feature_group.ingest(data_frame=to_feature_store, max_workers=3, wait=True)
