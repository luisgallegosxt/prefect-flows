from prefect import task, Flow, Parameter, unmapped
from prefect.executors import DaskExecutor
from prefect.schedules import CronSchedule
from prefect.engine import signals, state
from prefect.tasks.prefect import StartFlowRun
import pendulum
import prefect

import glob
import os
import datetime
import requests
import subprocess
import oracledb
import uuid


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders


@task
def insert_into_oracle(df):
	"""Insert into oracle table from list"""
	"""https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#installing-cx-oracle-on-windows"""
	## construct an insert statement that add a new row to the billing_headers table
	logger = prefect.context.get("logger")
	logger.info("Start process insert into oracle")
	## Initialize oracle client
	try:
		oracledb.init_oracle_client(lib_dir='C:\\oracle\\instantclient_21_6\\')
	except oracledb.ProgrammingError as e:
		logger.warning(e)

    ## Example tuple of list
	tuples = [('10','TEST',1 , 8 , 15 , 10, 9 , 1900), ('10','TEST1',1 , 8 , 15 , 11, 9 , 1900)]
    
    ## In case you have a dataframe
	# df = df['df'].values
	# tuples = [tuple(x) for x in df]

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


with Flow("insert-into-oracle") as flow:

	##Oracle
	username = USERNAME
	password = PASSWORD
		
	insert_into_oracle = insert_into_oracle()
	
flow.register(project_name="oracle")