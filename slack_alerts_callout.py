from prefect import task, Flow
from prefect.engine import state
from prefect.tasks.prefect import StartFlowRun
import pendulum

import os
import requests
import subprocess
import uuid

## callout a existing slack-alert-prefect-flow
def notify_on_fail(task, old_state, new_state):

	if isinstance(new_state, state.Failed):

		str_old_state = str(old_state.message)
		str_new_state = str(new_state.message)
		msg = f"Flow changed state from {str_old_state} to {str_new_state}"
		dict_param = {
			"flow_name": FLOW_NAME,
			"channel": SLACK_CHANNEL,
			"message": msg

			}
		slack = StartFlowRun("slack-alerts", project_name='alerts', wait=False)
		slack.run(parameters=dict_param, idempotency_key=uuid.uuid4().hex)
		
	return new_state


## Single http request to the slack api
def slack(text):
	now = pendulum.now("America/Guayaquil")
	fecha = now.strftime('%Y%m%d_%H%M%S')
	headers = {
	'Authorization': SLACK_TOKEN,
	'Content-type': 'application/json',
	}
	data = '{"channel":%s,"text":"%s: %s"}' % (SLACK_CHANNEL, fecha, text)
	response = requests.post('https://slack.com/api/chat.postMessage', headers=headers, data=data)


@task
def dummy_task():
	## You dont need to call notidfy_on_fail, it purpose is to call it on state_hadlers in the with parameter

	## You can call the slack fuction stand alone
	slack("Some awesome message")


with Flow("slack-alerts-callout1", state_handlers=[notify_on_fail]) as flow:

	dummy_task = dummy_task()
	
flow.register(project_name="slack-alerts-callout")