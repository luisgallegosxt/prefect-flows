from prefect import task, flow, get_run_logger
from prefect.deployments import Deployment

import requests
import json
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv
import os

env_path = '/datos/scripts2/internos/.env'
load_dotenv(dotenv_path=env_path)

jira_username=os.getenv("JIRA_USERNAME")
jira_token=os.getenv("JIRA_TOKEN")
jira_url=os.getenv("JIRA_URL")

path = '/path/to/code/'

# Endpoint for issues
search_url = f"{jira_url}/rest/api/3/search"

# Authentication
username = jira_username
api_token = jira_token

@task
def get_data():
	# Headers
	headers = {
		"Accept": "application/json",
		"Content-Type": "application/json"
	}

	# Payload
	query = {
		"jql": "project = <your_project>",  # Change this for your project
		"maxResults": 50,  # max num of rows
		"fields": ["summary", "status", "assignee", "created", "updated", "labels"]
	}

	# Request
	response = requests.get(search_url, headers=headers, auth=(username, api_token), params=query)
	# Init dataframes
	jira_issue_cab = pd.DataFrame(columns=["Issue_Key", "Summary", "Assignee", "created_at"])
	jira_issue_det = pd.DataFrame(columns=["Issue_Key", "From", "To", "Date"])
	jira_issue_tags = pd.DataFrame(columns=["Issue_Key", "Tag"])
	# Verify request
	if response.status_code == 200:
		issues = response.json()["issues"]
		for issue in issues:
			issue_key = issue['key']
			summary = issue['fields']['summary']
			assignee = issue['fields']['assignee']['displayName'] if issue['fields']['assignee'] else 'Sin asignar'
			created = issue['fields']['created']
			tags = issue['fields']['labels']  # Labels

			# Add this info to the dataframe
			temp_cab = pd.DataFrame([{
				"Issue_Key": issue_key,
				"Summary": summary,
				"Assignee": assignee,
				"created_at": datetime.strptime(created, '%Y-%m-%dT%H:%M:%S.%f%z')
			}])

			# Concat the df's
			jira_issue_cab = pd.concat([jira_issue_cab, temp_cab], ignore_index=True)

			# Add the labels to the labes dataframe
			for tag in tags:
				temp_tag = pd.DataFrame([{
					"Issue_Key": issue_key,
					"Tag": tag
				}])
				jira_issue_tags = pd.concat([jira_issue_tags, temp_tag], ignore_index=True)

			# Request to the history of the issue
			issue_url = f"{jira_url}/rest/api/3/issue/{issue_key}?expand=changelog"
			issue_response = requests.get(issue_url, headers=headers, auth=(username, api_token))

			if issue_response.status_code == 200:
				changelog = issue_response.json().get('changelog', {})
				histories = changelog.get('histories', [])
				status_history = []
				for history in histories:
					for item in history['items']:
						if item['field'] == 'status':
							# Añade la información de historial al DataFrame de detalle
							temp_det = pd.DataFrame([{
								"Issue_Key": issue_key,
								"From": item['fromString'],
								"To": item['toString'],
								"Date": datetime.strptime(history['created'], '%Y-%m-%dT%H:%M:%S.%f%z') 
							}])
							# Concat
							jira_issue_det = pd.concat([jira_issue_det, temp_det], ignore_index=True)
		
		jira_issue_cab['created_at'] = jira_issue_cab['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
		jira_issue_det['Date'] = jira_issue_det['Date'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

	else:
		print(f"Error: {response.status_code}")

	return jira_issue_cab, jira_issue_det, jira_issue_tags


@task
def export_to_datawarehouse(jira_issue_cab, jira_issue_det, jira_issue_tags):
    
    # Import to your favorite datawarehouse
    pass    


@flow
def jira_get_api_data():
	
	jira_issue_cab, jira_issue_det, jira_issue_tags = get_data()

	export_to_datawarehouse(jira_issue_cab, jira_issue_det, jira_issue_tags)


deployment = Deployment.build_from_flow(
	flow = jira_get_api_data,
	name = 'jira_get_api_data',
	path = '/path/to/code/',
	work_queue_name = 'default',
	work_pool_name = 'default-agent-pool',
	tags = ['jira']
)
deployment.apply()