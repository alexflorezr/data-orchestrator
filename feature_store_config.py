import re

config = {}

config['preprocessing'] = {
    'input_bucket_name':'dixa-labs-data-science',
    'input_key': 'data-lake/conversations/0.json',
    'output_bucket_name': 'airflow-temporal-objects',
    'output_prefix': 'warehouse',
    'output_file_name': re.sub('/', '_', re.sub('data-lake/|.json', '', 'data-lake/conversations/0.json'))
}
    
config['feature_store'] = {
    'feature_group_name':'conversation-feature-new',
    'identifier': 'id',
    'sagemaker_role_name': 'service-role/mwaa-environment-public-network-MwaaExecutionRole-17LYEO92FDZVE',
    'input_bucket_name': 'airflow-temporal-objects',
    'input_prefix': 'warehouse/',
    'type_definitions' : {
        'string': ['text', 'id', 'organization_id', 'direction', 'initial_channel', 'created_at_dt', 'created_at_raw'],
        'int64': ['dummy_idx']
    },
    'feature_store_bucket': 'offline-feature-store-test',
    'feature_store_prefix': 'dummy_test',
    'event_time' : 'created_at_raw'
}