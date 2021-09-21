import boto3
import pandas as pd
import json
import re
import time
from datetime import datetime


def hits_to_df(conversation):
    '''Takes the conversation (JSON format) stored in the S3 data lake and extract specific parts:
    dummy_idx = a dummy index (just for development purposes)
    text = the message text
    id = conversation id
    organization_id = client id
    direction = inbound or outbound
    initial_channel = email, widgetchat ...
    created_at = date of the first event in the conversation
    '''
    data_dict = {}
    data_dict['dummy_idx'] = []
    data_dict['text'] = []
    data_dict['id'] = []
    data_dict['organization_id']  = []
    data_dict['direction'] = []
    data_dict['initial_channel'] = []
    data_dict['created_at_dt'] = []
    data_dict['created_at_raw'] = []
    for i,hh in enumerate(conversation['hits']['hits']):
        convo_hits = hh['inner_hits']['message']['hits']['hits']
        for ii,h in enumerate(convo_hits):
            data_dict['dummy_idx'].append(i)
            data_dict['text'].append(h['_source']['text'])
            data_dict['id'].append(h['_source']['id'])
            data_dict['organization_id'].append(h['_source']['organization_id'])
            data_dict['initial_channel'].append(h['_source']['initial_channel'])
            data_dict['created_at_dt'].append(datetime.strptime(h['_source']['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ'))
            data_dict['created_at_raw'].append(h['_source']['created_at'])
            if 'direction' in h['_source'].keys():
                data_dict['direction'].append(h['_source']['direction'])
            else: 
                 data_dict['direction'].append('not_available')
    convo_df = pd.DataFrame.from_dict(data_dict)
    convo_df['text'] = convo_df['text'].map(lambda x: re.sub('\\n', ' ', x)) # Remove trails
    convo_df = convo_df.loc[convo_df['direction'] != 'not_available'] # Remove conversations without direction
    convo_df['direction'] = convo_df['direction'].str.lower() # Standarize direction lables to lower
    convo_df.reset_index(inplace=True, drop=True)
    return(convo_df)

def executable(input_bucket_name,
         input_key,
         output_bucket_name,
         output_prefix,
         output_file_name):
    s3_client = boto3.client('s3')
    objects = s3_client.get_object(Bucket=input_bucket_name, Key=input_key)
    conversation = json.loads(objects['Body'].read())
    convo_raw_data = hits_to_df(conversation)
    convo_raw_data.to_csv('s3://{}/{}/{}.csv'.format(output_bucket_name, output_prefix, output_file_name), index=False)   
     
