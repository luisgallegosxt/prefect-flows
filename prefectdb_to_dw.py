from prefect import task, Flow
import prefect
from pandas.tseries.offsets import BDay
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


@task(log_stdout=True, task_run_name="Postgres: {table_name}")
def connect_postgres(table_name):
	"""Connect postgres"""
	logger = prefect.context.get("logger")
	logger.info("Starting connect postgres")
	logger.info("Table Name: "+ table_name)

	# Create an engine instance
	## dialect+driver://username:password@host:port/database
	alchemyEngine = create_engine('postgresql+psycopg2://<db_user>:<db_pass>@<ip>:<port>/<db_name>', pool_recycle=3600)

	# Connect to PostgreSQL server
	dbConnection = alchemyEngine.connect()

	# Read data from PostgreSQL database table and load into a DataFrame instance
	dataFrame = pd.read_sql("select * from \"{}\"".format(table_name), dbConnection)

	# pd.set_option('display.expand_frame_repr', False);
	return dataFrame


@task(task_run_name="DW: {table_name}")
def export_to_dw(dataframe, table_name):
	"""Export data to datawarehouse"""
	logger = prefect.context.get("logger")
	logger.info("Start export data to dw")

    ## Import to your favorite Datawarehouse :)


with Flow("export_prefectdb_to_dw") as flow:

    # You can choose the deseable tables. I exclude some big ones.
	table_list = ['agent', 
	'agent_config', 
	'alembic_version',
	'cloud_hook',
	'edge',
	'flow',
	'flow_group',
	'flow_run',
	#'flow_run_state',
	#'log',
	'message',
	'project',
	'task',
	#'task_run',
	'task_run_artifact',
	#'task_run_state',
	'tenant']
	
	connect_postgres = connect_postgres.map(table_list)

	export_to_dw = export_to_dw.map(connect_postgres, table_list)
	
flow.register(project_name="prefectdb")