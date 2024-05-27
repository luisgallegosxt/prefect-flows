from prefect import task, flow, get_run_logger
import pendulum

from datetime import datetime
import requests
from dotenv import load_dotenv
import os

env_path = '/path/to/env/.env'
load_dotenv(dotenv_path=env_path)

slack_token=os.getenv("SLACK_TOKEN")

@task(log_stdout=True)
def slack(flow_name, channel, message):
	now = pendulum.now("your_time_zome")
	fecha = now.strftime('%Y%m%d_%H%M%S')
	headers = {
	'Authorization': slack_token,
	'Content-type': 'application/json',
	}
	data = '{"channel":"%s","text":"%s - %s", "attachments": [{"text": "%s"}]}' % (channel, fecha, flow_name, message)
	response = requests.post('https://slack.com/api/chat.postMessage', headers=headers, data=data)

@flow(flow_run_name="{flow_name} - {fecha}")
def slack_alerts(flow_name: str, channel: str, message: str):

	slack = slack(flow_name, channel, message)


deployment = Deployment.build_from_flow(
    flow = slack_alerts,
	name = "slack_alerts",
	path = '/path/to/code/',
	work_queue_name = 'default',
	work_pool_name = 'default-agent-pool',
	tags = ['alerts']
)
deployment.apply()
											  
