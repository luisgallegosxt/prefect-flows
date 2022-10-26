from prefect import task, Flow, Parameter
from prefect.tasks.prefect.flow_run_rename import RenameFlowRun
import pendulum

from datetime import datetime
import requests


@task
def generate_new_name(param):
	current_time = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
	run_name = f"{param} - {current_time}"

	return run_name


@task(log_stdout=True)
def slack(flow_name, channel, message):
	now = pendulum.now("America/Guayaquil")
	fecha = now.strftime('%Y%m%d_%H%M%S')
	headers = {
	'Authorization': SLACK_TOKEN,
	'Content-type': 'application/json',
	}
	data = '{"channel":"%s","text":"%s - %s", "attachments": [{"text": "%s"}]}' % (channel, fecha, flow_name, message)
	response = requests.post('https://slack.com/api/chat.postMessage', headers=headers, data=data)


with Flow("slack-alerts") as flow:

	flow_name = Parameter('flow_name')
	channel = Parameter('channel')
	message = Parameter('message')

	rename_flow = RenameFlowRun()(flow_run_id=None, flow_run_name=generate_new_name(flow_name))

	slack = slack(flow_name, channel, message)

flow.register(project_name="alerts")
											  
