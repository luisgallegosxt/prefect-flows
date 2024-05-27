from prefect import task, flow, get_run_logger
from prefect.client.orchestration import get_client
from prefect.deployments import Deployment
import anyio

from dotenv import load_dotenv
import os

env_path = '/path/to/env/.env'
load_dotenv(dotenv_path=env_path)

prefect_flow_deployment_id=os.getenv("PREFECT_FLOW_DEPLOYMENT_ID")

@task
async def exec_subflow(param_dict):
	async with get_client() as client:
		response = await client.create_flow_run_from_deployment(deployment_id=prefect_flow_deployment_id, parameters=param_dict)
		
		flow_run_id = response.id
		
		while True:
			flow_run = await client.read_flow_run(flow_run_id)
			flow_state = flow_run.state
			if flow_state and flow_state.is_final():
				return flow_run
			await anyio.sleep(2)

@flow
async def orchestrator_flow():
	# A list of dict: The key's should be the same name of the pre existing single-flow. 
	list_parameter = [{'name': 'param1'},
						{'name': 'param2'},
						{'name': 'paramn'}]

	for param in list_parameter:
		await ejecutar_flujo_hijo(param)


deployment = Deployment.build_from_flow(
    flow = orchestrator_flow,
    name = "orchestrator_flow",
	path = '/path/to/code/',
	work_queue_name = 'default',
	work_pool_name = 'default-agent-pool',
	tags = ['orchestrator']
)
deployment.apply()