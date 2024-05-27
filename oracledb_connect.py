from prefect import task, flow, get_run_logger

import oracledb

from dotenv import load_dotenv
import os

env_path = '/path/to/env/.env'
load_dotenv(dotenv_path=env_path)

oracle_username=os.getenv("ORACLE_USERNAME")
oracle_password=os.getenv("ORACLE_PASSWORD")

@task
def insert_into_oracle(df):
	"""Insert into oracle table from list"""
	"""https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#installing-cx-oracle-on-windows"""
	## construct an insert statement that add a new row to the billing_headers table
	logger = get_run_logger()
	logger.info("Start process insert into oracle")
	## Initialize oracle client
	try:
		oracledb.init_oracle_client(lib_dir='C:\\oracle\\instantclient_21_6\\')
	except oracledb.ProgrammingError as e:
		logger.warning(e)

    ## Example tuple of list
	tuples = [('10','TEST',1 , 8 , 15 , 10, 9 , 1900), ('10','TEST1',1 , 8 , 15 , 11, 9 , 1900)]


	# SQL query
	sql_query_insert_to_oracle = """insert into <SCHEMA>.<TABLE> (col1, col2, col3, col4, col5, col6, col7, col8)
								values (:1, :2, :3, :4, :5, :6, :7, :8)"""
    ## dsn example //<domain>:<port>/<service_name>                            
	dsn = DSN_STRING
	with oracledb.connect(user=username, password=password, dsn=dsn) as connection:
		# establish a new connection
		with connection.cursor() as cursor:
			# create a cursor
			with connection.cursor() as cursor:
				# execute the insert statement
				cursor.executemany(sql_query_insert_to_oracle, tuples)
				## SELECT Example
                # cursor.execute("SELECT * FROM TABLE")
				# commit work
				connection.commit()

 
@flow
def oracledb_connect():

	##Oracle
	username = USERNAME
	password = PASSWORD
		
	insert_into_oracle = insert_into_oracle()
	
deployment = Deployment.build_from_flow(
    flow = oracledb_connect,
    name = "oracledb_connect",
	path = '/path/to/code/',
	work_queue_name = 'default',
	work_pool_name = 'default-agent-pool',
	tags=['oracle']
)
deployment.apply()