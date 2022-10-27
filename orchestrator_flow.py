from prefect import Flow, unmapped
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from datetime import timedelta

import os
import re
import subprocess
import json
import uuid


with Flow("orchestrator-flows") as flow:
	# A list of dict: The key's should be the same name of the pre existing single-flow. 
	list_parameter = [{'name': 'param1'},
						{'name': 'param2'},
						{'name': 'paramn'}]

	# run_parameters takes a list of dicts of parameters to run your-csv-flow with
	load_flow = create_flow_run.map(flow_name=unmapped("single-flow"), project_name=unmapped("project"), parameters=list_parameter)

	# Wait for workers
	wait_for_flow_run = wait_for_flow_run.map(flow_run_id=load_flow)

flow.register(project_name="orchestrator")