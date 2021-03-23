#util
""" 
Customer Classification: segmentUtil
@autor: Cristopher Manriquez
@description: Utility functions needed across the entire Customer classification
process

"""
###############################################################################
#                           Libraries and Imports                             #
###############################################################################
import logging
import time
import os
import time 
import glob
import pandas as pd
import multiprocessing
from joblib import Parallel, delayed
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sklearn.tree import _tree
import sklearn.tree

import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
from google.cloud import storage

###############################################################################
#                           Functions Definitions                             #
###############################################################################
def getDates(process_period, months):
    """ 
    getDates Function: 
    Return the set of dates needed for the segmentation process
    
    processPeriod int: year month corresponding to the period to evaluate. Format: YYYYMM
    months int: quantity of months of Data to use in the Process.
    Return Dictionary with date values in string. Format YYYY-MM-DD
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: getDates({},{})".format(process_period, months))

        output_format = "%Y-%m-%d"
        date_to = datetime.strptime(str(process_period)+'01', "%Y%m%d")
        date_from = date_to + relativedelta(months=-months)
        date_to_inclusive = date_to - timedelta(days=1)
        date_to = date_to.strftime(output_format)
        date_from = date_from.strftime(output_format)
        date_to_inclusive = date_to_inclusive.strftime(output_format)
        
        # End log
        logging.info("Finishing Function: getDates || Time: {} sec.".format(str(round(time.time()-start))))

        return {'date_from': date_from,
                'date_to': date_to,
                'date_to_inclusive': date_to_inclusive,
                'period': process_period}
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def getBQClient():
    """ 
    getBQClient Function: 
    Return a Client object needed to execute querys and statements using BigQuery.
    In order to run this method in a local environment, is requeried to create 
    a service Key and the environment variable GOOGLE_APPLICATION_CREDENTIALS 
    with the path to the JSON File Key.
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: getBQClient()")

        credentials, my_project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        bqclient = bigquery.Client(credentials=credentials, project=my_project)
        
        bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(credentials=credentials)
        
        # End log
        logging.info("Finishing Function: getBQClient || Time: {} sec.".format(str(round(time.time()-start))))

        return (bqclient, bqstorageclient)
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def bqCheckTableExists(table_name, gcp_project, gcp_dataset, bqclient):
    """ 
    bqCheckTableExists Function: 
    Check in the given DataSet <gcp_dataset> the existence of a given table_name.
    Returns True if Table exists.
    
    table_name string: Name of the table to consult.
    gcp_project string: GCP Project  ID
    gcp_dataset string: BigQuery Dataset Name
    bqclient Client:    BigQuery Client.
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: bqCheckTableExists({})".format(table_name))

        query_string = """ 
                    SELECT size_bytes 
                    FROM `{0:s}:{1:s}.__TABLES__` 
                    WHERE table_id='{2:s}'
                    ;
        """.format(gcp_project, gcp_dataset,table_name)
        df_result = bqclient.query(query_string).result().to_dataframe()
        cnt = len(df_result.index)
        if cnt > 0 :
            logging.info("bqCheckTableExists: Table {} does exists".format(table_name))
            logging.info("End Function: bqCheckTableExists({})".format(table_name))
            return True
        else:
            logging.info("bqCheckTableExists: Table {} does not exists".format(table_name)) 
            logging.info("End Function: bqCheckTableExists({})".format(table_name))
            return False
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

###############################################################################
#                                GCP Functions                                #
###############################################################################
def check_bucket_exists(bucket_name):
    """ 
    check_bucket_exists Function: 
    Return True when bucket_name exists. False if it doesn't
    """    
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        return True
    except Exception as e:
        return False

def create_temp_bucket(bucket_name):
    """ 
    create_temp_bucket Function: 
    Creates 'bucket_name' bucket in GCP 
    """    
    try:
        storage_client = storage.Client()
        bucket = storage.Bucket(client=storage_client, name=bucket_name)
        bucket.create(client=storage_client, location='us-east1')
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
        
def bigquery_table_to_storage(gcp_project, gcp_dataset, table_name, bucket_name):
    """ 
    bigquery_table_to_storage Function: 
    Export a bigquery table to Google Storage in Gzip format
    """     
    try:
        bqclient = bigquery.Client()

        destination_uri = "gs://{}/{}".format(bucket_name, table_name+"*.gz")
        dataset_ref = bqclient.dataset(gcp_dataset, project=gcp_project)
        table_ref = dataset_ref.table(table_name)

        #Optional 
        job_config = bigquery.job.ExtractJobConfig()
        job_config.compression = 'GZIP'

        extract_job = bqclient.extract_table(
            table_ref,
            destination_uri,
            job_config = job_config,
            location="us-east1"
        )  # API request

        start = time.time()
        logging.info('bigquery_table_to_storage: Exporting table `{}.{}.{}` to Bucket {}'.format(gcp_project, gcp_dataset, table_name, bucket_name))
        extract_job.result()  # Waits for job to complete.
        logging.info('bigquery_table_to_storage: Table `{}.{}.{}` exported to Bucket {}'.format(gcp_project, gcp_dataset, table_name, bucket_name))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise        

def download_full_bucket(blob_name, destination_folder, bucket_name):
    """ 
    download_full_bucket Function: 
    Download all the files in a bucket to the local file system
    """  
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(destination_folder+blob.name)
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise        

def download_storage_files(bucket_name, table_name):
    """ 
    download_storage_files Function: 
    Download all the files in a bucket to the local file system
    """  
    try:
        storage_client = storage.Client()
        num_cores = multiprocessing.cpu_count()

        #check if prefix folder exists:
        if not os.path.exists(table_name):
            os.makedirs(table_name)
        destination_folder = table_name+'/'
        bucket = storage_client.get_bucket(bucket_name)
        blobs  = bucket.list_blobs(prefix=table_name) #List all objects that satisfy the filter.
        logging.info("download_storage_files: Downolading files prefix='{}%' Bucket {} to local File system {}/ ".format(table_name, bucket_name, destination_folder))
        results = Parallel(n_jobs=num_cores)(delayed(download_full_bucket)(blob.name, destination_folder, bucket_name) for blob in blobs)
        logging.info("download_storage_files: Bucket {} downloaded to local File system {}/ ".format(bucket_name, destination_folder))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
    
def delete_folder(folder_name):
    """ 
    delete_folder Function: 
    Delete a folder in the local file system and all its content 
    """      
    try:
        files = os.listdir(folder_name)
        for file in files:
            os.remove(folder_name+'/'+file)
        os.rmdir(folder_name)
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def folder_to_dataframe(folder):
    """ 
    folder_to_dataframe Function: 
    Reads all the content of a folder into a DataFrame 
    """  
    try:
        files = glob.glob("{}/*.gz".format(folder))
        dfs = [pd.read_csv(f) for f in files]
        data = pd.concat(dfs,ignore_index=True)
        return data
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def folder_to_dataframe_ext(folder, ext):
    """ 
    folder_to_dataframe Function: 
    Reads all the content of a folder into a DataFrame 
    """  
    try:
        files = glob.glob("{}/*.{}".format(folder, ext))
        dfs = [pd.read_csv(f) for f in files]
        data = pd.concat(dfs,ignore_index=True)
        return data
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
    
def delete_complete_bucket(bucket_name):
    """ 
    delete_complete_bucket Function: 
    Deletes a bucket in GCS and all its content
    """      
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            blob.delete()
        bucket.delete()
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
    
def load_table_to_dataframe(gcp_project, gcp_dataset, table_name, bucket_name):
    """ 
    load_table_to_dataframe Function: 
    Takes a table in Big Query and moves it to Google Cloud Storage. 
    Then downloads the data from Storage to the local file system 
    to finally return a data frame with the data.
    """     
    try:
        bigquery_table_to_storage(gcp_project, gcp_dataset, table_name, bucket_name)
        download_storage_files(bucket_name, table_name)
        data = folder_to_dataframe(table_name)
        delete_folder(table_name)
        return data
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise    
        
def extract_thresholds(node, obj_tree, feature_names, thresholds={}):
    """ 
    extract_thresholds Function: 
    Goes through a tree object to return a Dictionary with all the thresholds used in it
    """     
    try:
        if obj_tree.feature[node] != _tree.TREE_UNDEFINED:
            thresholds[feature_names[obj_tree.feature[node]]] = obj_tree.threshold[node]
            thresholds = extract_thresholds(obj_tree.children_left[node], obj_tree, feature_names, thresholds)
            thresholds = extract_thresholds(obj_tree.children_right[node], obj_tree, feature_names, thresholds)
        return thresholds
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 
        
def dropTemporalTables(country_id, period, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    dropTemporalTables Function:
    Drop all temporal tables.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: dropTemporalTables({}, {}, {}, {})".format(gcp_project, gcp_dataset, country_id, str(period)))    

		###############################################################################
		#                           Drop TMP Segmentation                             #
		###############################################################################
        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_trainingset_{}{}_light`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()
        
        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_scoringset_{}{}_light`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()
        
        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_trainingset_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()
        
        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_training_test_results_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()
        
        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_scoringset_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()
        
        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_fctrdata_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_fctrhist_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_ggeehist_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_bltahog_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_fctrpro_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_all_customers_ve_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_vehist_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_customers_blta_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_all_customers_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_features_set_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_tmp_totals_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.segmentation_features_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

		###############################################################################
		#                           Drop TMP pronecessity                             #
		###############################################################################
        drop_str = "DROP TABLE IF EXISTS `{}.{}.pronecessity_lvl2_training_customers_noout_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.pronecessity_lvl2_all_customers_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.pronecessity_lvl2_features_set_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        drop_str = "DROP TABLE IF EXISTS `{}.{}.pronecessity_lvl2_trainingset_{}{}`".format(gcp_project, gcp_dataset, country_id, str(period))
        logging.info("SQL: "+drop_str)
        bqclient.query(drop_str).result()

        # End log
        logging.info("Finishing Function: createTemporalVEECustomers || Time: {} sec.".format(str(round(time.time()-start)))) 
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise