import textwrap
from datetime import datetime, timedelta
import uuid
import os 
import sys 
import shutil
from elasticsearch import Elasticsearch

from airflow import DAG 
from airflow import macros  
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.dummy_operator import DummyOperator
from pyhocon import ConfigFactory

from databuilder.publisher.elasticsearch_constants import DASHBOARD_ELASTICSEARCH_INDEX_MAPPING, USER_ELASTICSEARCH_INDEX_MAPPING
from databuilder.extractor.bigquery_metadata_extractor import BigQueryMetadataExtractor
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.job.job import DefaultJob
from databuilder.extractor.neo4j_es_last_updated_extractor import Neo4jEsLastUpdatedExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer


dag_args = {
    'concurrency': 10,
    # One dagrun at a time
    'max_active_runs': 1,
    # 4AM, 4PM PST
    'schedule_interval': '0 11 * * *',
    'catchup': False
}

default_args = {
    'owner': 'Salma Amr',
    'start_date': datetime(2020, 9, 16),
    'depends_on_past': False,
    'email': ['salma.abdelfattah@talabat.com'],
    'email_on_failure': True,
    'bigquery_conn_id': 'bigquery_default'
}

es_host = 'local_host'
neo_host = 'local_host'


es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)


es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

NEO4J_ENDPOINT = 'bolt://{}:{}'.format(neo_host, neo_port)

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'


def create_table_extract_job(**kwargs):

    tmp_folder = '/var/tmp/amundsen/{metadata_type}'.format(metadata_type=kwargs['metadata_type'])
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)
    
    bq_meta_extractor = BigQueryMetadataExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=bq_meta_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        'extractor.bigquery_table_metadata.{}'.format(BigQueryMetadataExtractor.PROJECT_ID_KEY):
            kwargs['PROJECT_ID_KEY'],
        'extractor.bigquery_table_metadata.{}'.format(BigQueryMetadataExtractor.FILTER_KEY): #filter desired datasets only
        'labels.set_label:data_platform',
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR):
            True,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    
    job.launch()



def create_es_publisher_sample_job(**kwargs):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_{uuid}`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param entity_type:                Entity type handed to the `Neo4jSearchDataExtractor` class, used to determine
                                       Cypher query to extract data from Neo4j. Defaults to `table`.
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    elasticsearch_client = es
    elasticsearch_new_index_key = '{}_'.format(kwargs['elasticsearch_doc_type_key']) + str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.entity_type': kwargs['entity_type'],
        'extractor.search_data.extractor.neo4j.graph_url': neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.model_class': kwargs['model_name'],
        'extractor.search_data.extractor.neo4j.neo4j_auth_user': neo4j_user,
        'extractor.search_data.extractor.neo4j.neo4j_auth_pw': neo4j_password,
        'extractor.search_data.extractor.neo4j.neo4j_encrypted': False,
        'loader.filesystem.elasticsearch.file_path': extracted_search_data_path,
        'loader.filesystem.elasticsearch.mode': 'w',
        'publisher.elasticsearch.file_path': extracted_search_data_path,
        'publisher.elasticsearch.mode': 'r',
        'publisher.elasticsearch.client': elasticsearch_client,
        'publisher.elasticsearch.new_index': elasticsearch_new_index_key,
        'publisher.elasticsearch.doc_type': kwargs['elasticsearch_doc_type_key'],
        'publisher.elasticsearch.alias': kwargs['elasticsearch_index_alias'], })

    # only optionally add these keys, so need to dynamically `put` them
    elasticsearch_mapping = kwargs['elasticsearch_mapping']
    if elasticsearch_mapping:
        job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    job.launch()




def create_last_updated_job():
    # loader saves data to these folders and publisher reads it from here
    tmp_folder = '/var/tmp/amundsen/last_updated_data'
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    task = DefaultTask(extractor=Neo4jEsLastUpdatedExtractor(),
                       loader=FsNeo4jCSVLoader())

    job_config = ConfigFactory.from_dict({
        'extractor.neo4j_es_last_updated.model_class':
            'databuilder.models.neo4j_es_last_updated.Neo4jESLastUpdated',

        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_lastupdated_tag',  # should use unique tag here like {ds}
    })

    job = DefaultJob(conf=job_config,
                    task=task,
                    publisher=Neo4jCsvPublisher())
    job.launch()



with DAG('amundsen', default_args=default_args, **dag_args) as dag:

    start = DummyOperator(task_id="start_etl", dag=dag)
    end = DummyOperator(task_id="end_etl", dag=dag)

    bigquery_extract_job = PythonOperator(
        task_id='bigquery_extract_job',
        python_callable=create_table_extract_job,
        op_kwargs={'PROJECT_ID_KEY': 'PROJECT_ID_NAME', 'metadata_type': 'bigquery_metadata'})

    #update last_index
    last_update_job = PythonOperator(
        task_id='last_update_job',
        python_callable=create_last_updated_job
    )

    table_search_index_job = PythonOperator(
        task_id = 'table_search_index_job',
        python_callable = create_es_publisher_sample_job,
        op_kwargs={'elasticsearch_index_alias': 'table_search_index',
                    'elasticsearch_doc_type_key': 'table',
                    'entity_type': 'table',
                    'model_name': 'databuilder.models.table_elasticsearch_document.TableESDocument',
                    'elasticsearch_mapping': None}
    )

    user_search_index_job = PythonOperator(
        task_id = 'user_search_index_job',
        python_callable = create_es_publisher_sample_job,
        op_kwargs={'elasticsearch_index_alias': 'user_search_index',
                    'elasticsearch_doc_type_key': 'user',
                    'model_name': 'databuilder.models.user_elasticsearch_document.UserESDocument',
                    'entity_type': 'user',
                    'elasticsearch_mapping': USER_ELASTICSEARCH_INDEX_MAPPING}
    )

    dashboard_search_index_job = PythonOperator(
        task_id = 'dashboard_search_index_job',
        python_callable = create_es_publisher_sample_job,
        op_kwargs={'elasticsearch_index_alias': 'dashboard_search_index',
                    'elasticsearch_doc_type_key': 'dashboard',
                    'model_name': 'databuilder.models.dashboard_elasticsearch_document.DashboardESDocument',
                    'entity_type': 'dashboard',
                    'elasticsearch_mapping': DASHBOARD_ELASTICSEARCH_INDEX_MAPPING}
    )
    
    start >> bigquery_extract_job >> last_update_job >> table_search_index_job >> user_search_index_job >> dashboard_search_index_job >>  end








