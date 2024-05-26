from prefect import task, flow, get_run_logger
from prefect.deployments import Deployment
from datetime import datetime, timedelta

import requests
import json
import pandas as pd

from dotenv import load_dotenv
import os

env_path = '/path/to/env/.env'
load_dotenv(dotenv_path=env_path)

api_key=os.getenv("API_KEY")

def get_response_count(api_key):
	"""Get the responses count per request"""
	url = API_COUNT_URL

	querystring = {"apiKey": api_key}

	r = requests.request("GET", url, params=querystring)

	resp = json.loads(r.text)

	return resp


def get_responses(api_key, idx):
	"""Get the responses from api 100 per request in this example"""
	url = API_RESPONSES

	querystring  = {'page': idx, 'perPage': '100', 'apiKey': api_key}

	r = requests.request("GET", url, params=querystring)

	resp = json.loads(r.text)

	return resp


def transform_response(row):
	row_dumps_resp = json.dumps(row['responseSet'])
	row_list_resp = json.loads(row_dumps_resp)

	for resp in row_list_resp:
        ## do any data treatment and create a new column with your result
		row['any_column_name'] = 'any_treatment_result'
	return row


@task
def get_responses():
	logger = get_run_logger()
	logger.info("Start api call")
	total_df = pd.DataFrame()

	# Fetch the responses by the previous captured range
	total_responses_json = get_response_count(api_key)
	summarized_responses = int(total_responses_json['response']['completedResponses'])
	for idx, step in enumerate(range(0, summarized_responses, 100)):
		api_resp = get_responses(api_key, idx + 1)
		responses_dict = api_resp['response']
		df = pd.DataFrame.from_dict(responses_dict)
		total_df = pd.concat([total_df, df])
		logger.info('Success'+ str(idx))

	return total_df


@task
def extract_response_task(df):
	logger = prefect.context.get("logger")
	logger.info("Start dataframe extract")

	df_output = df.apply(transform_response, axis='columns')
    ## Droo innecesary colums
	df_output = df_output.drop(['responseSet'], axis=1)
    ## Rename columns name
	df_output = df_output.rename(mapper=str.strip, axis='columns')
    ## Some prefix and suffix columns rename
	df_output = df_output.add_suffix('_post')
	df_output = df_output.add_prefix('pre_')
    ## convert all values in string
	df_output = df_output.applymap(str)
    ## If you want to select specific column names
	df_output = df_output[['pre_column1_post', 'pre_column2_post', 'pre_columnn_post']]
	
	return df_output

@flow
def api_http_request():

	api_key = api_key
	
	get_responses = get_responses()

	extract_response_task = extract_response_task(get_responses)

deployment = Deployment.build_from_flow(
    flow = api_http_request,
    name = "api_http_request",
	path = '/path/to/code/',
	work_queue_name = 'default',
	work_pool_name = 'default-agent-pool',
	tags=['api']
)
deployment.apply()

