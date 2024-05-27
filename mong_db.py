from prefect import task, flow, get_run_logger
from prefect.deployments import Deployment

import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

env_path = '/path/to/env/.env'
load_dotenv(dotenv_path=env_path)

mongo_host=os.getenv("MONGO_HOST")
mongo_username=os.getenv("MONGO_USERNAME")
mongo_password=os.getenv("MONGO_PASSWORD")
mongo_db=os.getenv("MONGO_DB")
mongo_schema=os.getenv("MONGO_SCHEMA")

@task
def read_mongo(db, collection, query, host=mongo_host, port=47057, username=mongo_username, password=mongo_password, no_id=True):
	""" Read from Mongo and Store into DataFrame """

	if username and password:
		mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
		conn_mongo = MongoClient(mongo_uri)
	else:
		conn_mongo = MongoClient(host, port)

	conn_mongo = conn_mongo[db]
	cursor = conn_mongo[collection].find(query)
	df_mongo =  pd.DataFrame(list(cursor))
	
    return df_mongo


@task
def import_to_dw(dataframe):
    # Import to your favorite datawarehouse
    pass
	  

@flow
def import_from_mongodb():
    query = {}

    read_mongo_1 = read_mongo(mongo_db, mongo_schema, query)

	import_to_dw(read_mongo_1)

deployment = Deployment.build_from_flow(
	flow = import_from_mongodb,
	name = "import_from_mongodb",
	path = '/path/to/code/',
	work_queue_name = 'default',
	work_pool_name = 'default-agent-pool',
	tags = ['mongo']
)
deployment.apply()