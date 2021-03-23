#features
""" 
Customer Classification: segmentfeatures
@autor: Cristopher Manriquez
@description: Functions involving the creation and management of features and 
variables needed across for the  Customer classification process

"""
###############################################################################
#                           Libraries and Imports                             #
###############################################################################
from segmentutil import bqCheckTableExists
import logging
import time

###############################################################################
#                          Insert Feature Functions                           #
###############################################################################
def createFeaturesTable(country_id, period, gcp_project, gcp_dataset, bqclient):
    """
    createFeaturesTable: 
    Creates empty table to store the features to use in the segmentation process.
    
	    country_id str: Country to process
	    period int: Period to process. FORMAT YYYYMM
	    gcp_project str: GCP project to execute the process
	    gcp_dataset str: GCP dataset to store the process
	    bqclient obj: BigQuery Client object needed to connect and execute 
	        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createFeaturesTable({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))

        #define table name
        table_name = "segmentation_features_{}{}".format(country_id,str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}.{}.{}`".format(gcp_project,gcp_dataset,table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        query_string = """
            CREATE TABLE `{}.{}.{}` 
            (
                customer_id STRING,
                feature_name STRING,
                feature_value FLOAT64
            )
        """.format(gcp_project, gcp_dataset, table_name)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: createFeaturesTable || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def createTemporalTotalsTable(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """
    createTemporalTotalsTable Fucntion:
    Calculate total valuefor different variables needed to calculate some features.
    
	    gcp_OH_tables Dict: Dictionary with the OH table names        
	    country_id str: Country to process
	    period int: Period to process. FORMAT YYYYMM
	    date_from str: date to start considering data. FORMAT YYYY-MM-DD
	    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
	    customers_set_table str: name for the table of customers to calculate variables
	    gcp_project str: GCP project to execute the process
	    gcp_dataset str: GCP dataset to store the process

	    values to generate:
	        - total_units_cnt
	        - total_sales_amt
	        - total_transactions
	        - total_stores
	        - total_distinct_items
	        - total_distinct_sets
	    bqclient obj: BigQuery Cliente object needed to connect and execute 
	        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createTemporalTotalsTable({}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset))

        #prepare table name with project and dataset
        table_name = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))

        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        #create table 
        query_create = """
                            CREATE TABLE `{}` AS
                            SELECT  y.customer_id
                                    , SUM(CAST(f.unit_cnt AS FLOAT64))                AS total_units_cnt
                                    , SUM(CAST(f.st_extended_net_amt  AS FLOAT64))    AS total_sales_amt
                                    , CAST(COUNT(DISTINCT location_id) AS FLOAT64)    AS total_stores
                                    , CAST(COUNT(DISTINCT f.item_id) AS FLOAT64)      AS total_distinct_items
                                    , CAST(COUNT(DISTINCT it.hier_set_cd) AS FLOAT64) AS total_distinct_sets
                                    , CAST(COUNT(DISTINCT it.hier_department_cd) AS FLOAT64)/6 AS per_distinct_departments
                                    , CAST(COUNT(DISTINCT CONCAT(   f.country_id,
                                                                    f.business_unit_id,
                                                                    CAST(f.sales_return_dt AS STRING),
                                                                    f.sales_return_type_cd,
                                                                    CAST(f.sales_return_document_num AS STRING),
                                                                    CAST(f.location_id AS STRING))) AS FLOAT64) AS total_transactions
                                    , DATE_DIFF('{}', MAX(f.sales_return_dt), month) AS months_since_last_trx                                                                    
                            FROM `{}`    AS f
                            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                                                                                    AND f.business_unit_id = it.business_unit_id                         
                                                                                                    AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
                            INNER JOIN `{}`                              AS y    ON  CASE 
                                                                                                            WHEN TRIM(f.customer_id) = '0' 
                                                                                                              OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                                            ELSE TRIM(f.customer_id) 
                                                                                                        END = y.customer_id
                            WHERE  f.country_id = '{}'
                            AND f.business_unit_id = 'ENT'
                            AND f.sales_return_dt >= '{}' 
                            AND f.sales_return_dt < '{}'
                            AND f.st_extended_net_amt > 0 
                            AND f.unit_cnt > 0 
                            AND f.sales_return_type_cd IN ('blta', 'fctr')
                            GROUP BY y.customer_id
                            ;
                            """.format(table_name, 
                                       date_to, 
                                       gcp_OH_tables['sales_table'], 
                                       gcp_OH_tables['items_table'], 
                                       customers_set_table, 
                                       country_id, 
                                       date_from, 
                                       date_to)
        logging.info("SQL: "+query_create)
        bqclient.query(query_create).result()
        
        # End log
        logging.info("Finishing Function: createTemporalTotalsTable || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturesPerTransactions(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """
    insertPerTransactionsFeatures Fucntion:
    Calculate and insert transactions percentage features for the classification algorithm in different levels .
    Gets Data from sales_return_fact
    
	    gcp_OH_tables Dict: Dictionary with the OH table names
	    country_id str: Country to process
	    period int: Period to process. FORMAT YYYYMM
	    date_from str: date to start considering data. FORMAT YYYY-MM-DD
	    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
	    feature_level str: value or expression to use as a grouper in the query
	    customers_set_table str: name for the table of customers to calculate variables
	    gcp_project str: GCP project to execute the process
	    gcp_dataset str: GCP dataset to store the process
	    bqclient obj: BigQuery Cliente object needed to connect and execute 
	        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturesPerTransactions({}, {}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                {} AS feature_name,
                COUNT(DISTINCT CONCAT(  f.country_id,
                                        f.business_unit_id,
                                        CAST(f.sales_return_dt AS STRING),
                                        f.sales_return_type_cd,
                                        CAST(f.sales_return_document_num as STRING),
                                        CAST(f.location_id AS STRING )))/tot.total_transactions AS feature_value
            FROM `{}`    AS f
            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                                                                                    AND f.business_unit_id = it.business_unit_id                         
                                                                                                    AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}`                              AS cf ON CASE 
                                                                                        WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                     END = cf.customer_id
            INNER JOIN `{}`                                 AS tot ON CASE 
                                                                                        WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                     END  = tot.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND {} IS NOT NULL
            GROUP BY 
            feature_name,
            cf.customer_id,
            tot.total_transactions;
        """.format( features_table, 
                    feature_level, 
                    gcp_OH_tables['sales_table'],
                    gcp_OH_tables['items_table'],
                    customers_set_table, 
                    tmp_totals_table, 
                    country_id, 
                    date_from, 
                    date_to, 
                    feature_level)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        #End
        logging.info("Finishing Function: insertFeaturesPerTransactions || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerTrxHier(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerTrxHier Function:
    Calculates and inserts per_trx_hier_group_cd feature for the Classification Algorithm.

	    gcp_OH_tables Dict: Dictionary with the OH table names
	    country_id str: Country to process
	    period int: Period to process. FORMAT YYYYMM
	    date_from str: date to start considering data. FORMAT YYYY-MM-DD
	    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
	    feature_level str: value or expression to use as a grouper in the query
	    customers_set_table str: name for the table of customers to calculate variables
	    gcp_project str: GCP project to execute the process
	    gcp_dataset str: GCP dataset to store the process
	    bqclient obj: BigQuery Cliente object needed to connect and execute 
	        querys and statements in Google Cloud platform BigQuery environment
    """
    # Start log
    try:
        start = time.time()
        logging.info("Start Function: insertFeaturePerTrxHier({}, {}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                {} AS feature_name,
                COUNT(DISTINCT CONCAT(  f.country_id,
                                        f.business_unit_id,
                                        CAST(f.sales_return_dt AS STRING),
                                        f.sales_return_type_cd,
                                        CAST(f.sales_return_document_num as STRING),
                                        CAST(f.location_id AS STRING)))/tot.total_transactions AS feature_value
            FROM `{}`    AS f
            INNER JOIN `{}`       AS it  ON f.country_id = it.country_id 
                                                                                   AND f.business_unit_id = it.business_unit_id 
                                                                                   AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}`                              AS cf ON CASE
                                                                                        WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                    END = cf.customer_id
            INNER JOIN `{}`                                 AS tot ON CASE 
                                                                                       WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                    END = tot.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0
            AND {} IS NOT NULL
            GROUP BY 
                feature_name,
                cf.customer_id,
                tot.total_transactions
            ;
        """.format( features_table, 
                    feature_level, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    customers_set_table, 
                    tmp_totals_table, 
                    country_id, 
                    date_from, 
                    date_to, 
                    feature_level)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeaturePerTrxHier || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerUniHier(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerUniHier Function:
    Calculates and inserts per_uni_hier_group_cd feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        feature_level str: value or expression to use as a grouper in the query
        customers_set_table str: name for the table of customers to calculate variables
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerUniHier({}, {}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                {} AS feature_name,
                SUM(CAST(f.unit_cnt AS FLOAT64))/tot.total_units_cnt AS feature_value
            FROM `{}`    AS f
            INNER JOIN `{}`       AS it  ON f.country_id = it.country_id 
                                                                                   AND f.business_unit_id = it.business_unit_id 
                                                                                   AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}`                              AS cf ON CASE
                                                                                        WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                    END = cf.customer_id
            INNER JOIN `{}`                                 AS tot ON CASE 
                                                                                        WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                    END = tot.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0            
            AND {} IS NOT NULL
            GROUP BY 
                feature_name,
                cf.customer_id,
                tot.total_units_cnt
            ;
        """.format(features_table, 
                    feature_level, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    customers_set_table, 
                    tmp_totals_table, 
                    country_id, 
                    date_from, 
                    date_to,
                    feature_level)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeaturePerUniHier || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeatureTrxHier(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeatureTrxHier Function:
    Calculates and inserts trx_hier_group_cd feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        feature_level str: value or expression to use as a grouper in the query
        customers_set_table str: name for the table of customers to calculate variables
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureTrxHier({}, {}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                {} AS feature_name,
                COUNT(DISTINCT CONCAT(  f.country_id,
                                        f.business_unit_id,
                                        CAST(f.sales_return_dt AS STRING),
                                        f.sales_return_type_cd,
                                        CAST(f.sales_return_document_num as STRING),
                                        CAST(f.location_id AS STRING)
                                    )
                ) AS feature_value
            FROM `{}`    AS f
            INNER JOIN `{}`       AS it  ON f.country_id = it.country_id 
                                                                                   AND f.business_unit_id = it.business_unit_id 
                                                                                   AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}`                              AS cf ON CASE
                                                                                        WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                    END = cf.customer_id
            INNER JOIN `{}`                                 AS tot ON CASE 
                                                                                        WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                    END = tot.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0
            AND {} IS NOT NULL
            GROUP BY 
                feature_name,
                cf.customer_id
            ;
        """.format(features_table, 
                    feature_level, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    customers_set_table, 
                    tmp_totals_table, 
                    country_id, 
                    date_from, 
                    date_to,
                    feature_level)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeatureTrxHier || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerTrxNtcr(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerTrxNtcr Function:
    Calculates and inserts per_trx_ntcr feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        customers_set_table str: name for the table of customers to calculate variables
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerTrxNtcr({}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                'per_trx_ntcr' AS feature_name,
                COUNT(DISTINCT CONCAT(  f.country_id,
                                        f.business_unit_id,
                                        CAST(f.sales_return_dt AS STRING),
                                        f.sales_return_type_cd,
                                        CAST(f.sales_return_document_num as STRING),
                                        CAST(f.location_id AS STRING)
                                    )
                    )/tot.total_transactions AS feature_value
            FROM `{}`                              AS f
            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                                                                                    AND f.business_unit_id = it.business_unit_id                         
                                                                                                    AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}` AS cf ON CASE    
                                                            WHEN TRIM(f.customer_id) = '0' 
                                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                            ELSE TRIM(f.customer_id) 
                                                        END = cf.customer_id
            INNER JOIN `{}` AS tot ON   CASE  
                                                            WHEN TRIM(f.customer_id) = '0' 
                                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                            ELSE TRIM(f.customer_id) 
                                                        END = tot.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd = 'ntcr'
            GROUP BY 
                feature_name,
                cf.customer_id,
                tot.total_transactions
            ;
        """.format(features_table, gcp_OH_tables['sales_table'], gcp_OH_tables['items_table'], customers_set_table, tmp_totals_table, country_id, date_from, date_to)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeaturePerTrxNtcr || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerSalesNtcr(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerSalesNtcr Function:
    Calculates and inserts per_sales_ntcr feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        customers_set_table str: name for the table of customers to calculate variables
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerSalesNtcr({}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                'per_sales_ntcr' AS feature_name,
                SUM(CAST(f.st_extended_net_amt  as FLOAT64))/tot.total_sales_amt AS feature_value
            FROM `{}` AS f
            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                                                                                    AND f.business_unit_id = it.business_unit_id                         
                                                                                                    AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}` AS cf ON CASE  
                                                            WHEN TRIM(f.customer_id) = '0' 
                                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                            ELSE TRIM(f.customer_id) 
                                                        END = cf.customer_id
            INNER JOIN `{}` AS tot ON   CASE    
                                                            WHEN TRIM(f.customer_id) = '0' 
                                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                            ELSE TRIM(f.customer_id) 
                                                        END = tot.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd = 'ntcr'
            GROUP BY 
                feature_name,
                cf.customer_id,
                tot.total_sales_amt
            ;
        """.format(features_table, gcp_OH_tables['sales_table'], gcp_OH_tables['items_table'], customers_set_table, tmp_totals_table, country_id, date_from, date_to)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeaturePerSalesNtcr || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeatureSkusSubfamily(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeatureSkusSubfamily Function:
    Calculates and inserts skus_subfamily_hier_subfamily_cd feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        feature_level str: value or expression to use as a grouper in the query
        customers_set_table str: name for the table of customers to calculate variables
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureSkusSubfamily({}, {}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, feature_level, customers_set_table, gcp_project, gcp_dataset))    

        #prepare table names
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                {} AS feature_name,
                COUNT(DISTINCT f.item_id) AS feature_value    
            FROM `{}`    AS f
            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                                                                    AND f.business_unit_id = it.business_unit_id 
                                                                                    AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}`  AS cf ON CASE 
                                                            WHEN TRIM(f.customer_id) = '0' 
                                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                            ELSE TRIM(f.customer_id) 
                                                        END = cf.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0            
            AND {} IS NOT NULL
            GROUP BY 
                feature_name,
                cf.customer_id
            ;
        """.format( features_table, 
                    feature_level, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    customers_set_table, 
                    country_id, 
                    date_from, 
                    date_to, 
                    feature_level)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: insertFeatureSkusSubfamily || Time: {} sec.".format(str(round(time.time()-start))))    
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeatureSetDiversity(country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeatureSetDiversity Function:
    Calculates and inserts set_diversity feature for the Classification Algorithm.
    
	    country_id str: Country to process
	    period int: Period to process. FORMAT YYYYMM
	    date_from str: date to start considering data. FORMAT YYYY-MM-DD
	    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
	    customers_set_table str: name for the table of customers to calculate variables
	    gcp_project str: GCP project to execute the process
	    gcp_dataset str: GCP dataset to store the process
	    bqclient obj: BigQuery Cliente object needed to connect and execute 
	        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureSetDiversity({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'set_diversity' AS feature_name,
                tot.total_distinct_sets / tot.total_distinct_items AS feature_value
            FROM `{}` AS tot
            ;
        """.format(features_table, tmp_totals_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeatureSetDiversity || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeatureTotalStores(country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeatureTotalStores Function:
    Calculates and inserts total_stores feature for the Classification Algorithm.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    date_from str: date to start considering data. FORMAT YYYY-MM-DD
    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
    customers_set_table str: name for the table of customers to calculate variables
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureTotalStores({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))    

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'total_stores' AS feature_name,
                tot.total_stores AS feature_value
            FROM `{}` AS tot
            ;
        """.format(features_table, tmp_totals_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeatureTotalStores || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerTransactionMostFrecuentStore(country_id, period, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerTransactionMostFrecuentStore Function:
    Calculates and inserts the transaction percentage in the most frecuent Store per Customer.

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
        logging.info("Start Function: insertFeaturePerTransactionMostFrecuentStore({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT  customer_id,
                    'per_trx_cuchi_store' AS feature_name,
                    feature_value 
            FROM (
                SELECT
                    ft.customer_id,
                    'per_trx_cuchi_store' AS feature_name,
                    feature_value,
                    ROW_NUMBER() OVER(PARTITION BY ft.customer_id ORDER BY ft.feature_value DESC) AS RN
                FROM `{}` AS ft
                WHERE ft.feature_name like 'per_trx_store_%'
            )
            WHERE RN = 1
            ;
        """.format(features_table, features_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: insertFeaturePerTransactionMostFrecuentStore || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertAvgUnitsTrx(country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    insertAvgUnitsTrx Function:
    Calculates and inserts avg_units_trx feature for the Classification Algorithm.
    
	    country_id str: Country to process
	    period int: Period to process. FORMAT YYYYMM
	    date_from str: date to start considering data. FORMAT YYYY-MM-DD
	    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
	    customers_set_table str: name for the table of customers to calculate variables
	    gcp_project str: GCP project to execute the process
	    gcp_dataset str: GCP dataset to store the process
	    bqclient obj: BigQuery Cliente object needed to connect and execute 
	        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertAvgUnitsTrx({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'avg_units_trx' AS feature_name,
                tot.total_units_cnt/tot.total_transactions AS feature_value
            FROM `{}` AS tot;
        """.format(features_table, tmp_totals_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertAvgUnitsTrx || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturesSkuPro(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerTrxHier Function:
    Calculates and inserts per_trx_hier_group_cd feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        feature_level str: value or expression to use as a grouper in the query
        customers_set_table str: name for the table of customers to calculate variables
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud platform BigQuery environment
    """
    # Start log
    try:
        start = time.time()
        logging.info("Start Function: insertFeaturePerTrxHier({}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{0:s}`
            SELECT
                cf.customer_id,
                CONCAT('item_pro_', CAST(it.item_id as STRING)) AS feature_name,
                CASE WHEN COUNT(DISTINCT CONCAT(  f.country_id,
                                         f.business_unit_id,
                                         CAST(f.sales_return_dt AS STRING),
                                         f.sales_return_type_cd,
                                         CAST(f.sales_return_document_num as STRING),
                                         CAST(f.location_id AS STRING))) > 0 THEN 1 ELSE 0 END AS feature_value
            FROM `{1:s}`   AS f
            INNER JOIN `{2:s}` AS it  ON f.country_id = it.country_id 
                                                                                   AND f.business_unit_id = it.business_unit_id 
                                                                                   AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{3:s}` AS cf ON    CASE
                                                                                                                      WHEN TRIM(f.customer_id) = '0' 
                                                                                                                      OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                                                      ELSE TRIM(f.customer_id) 
                                                                                                                    END = cf.customer_id
            INNER JOIN `{4:s}` AS skupro   ON    f.country_id = skupro.country_id 
                                                                    AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(skupro.item_id AS INT64)
            WHERE  f.country_id = '{5:s}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{6:s}' 
            AND f.sales_return_dt < '{7:s}'
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0
            GROUP BY 
                feature_name,
                cf.customer_id
            ;
        """.format( features_table, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    customers_set_table, 
                    gcp_OH_tables['items_pro_table'], 
                    country_id, 
                    date_from, 
                    date_to)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeaturePerTrxHier || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

###############################################################################
#                     typeB by Necessity: Level 2                             #
#                           (L2A, L2B, L2C)                                   #
###############################################################################
def insertFeaturePerTrxRent(gcp_OH_tables, country_id, period, date_from, date_to, features_table, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerTrxRent Function:
    Calculates and inserts per_trx_rent feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute
            querys and statements in Google Cloud Plataform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerTrxRent({}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #customers_set_table = "{}.{}.segmentation_profile_results_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))

        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                'per_trx_rent' AS feature_name,
                COUNT(DISTINCT CONCAT(  f.country_id,
                                        f.business_unit_id,
                                        CAST(f.sales_return_dt AS STRING),
                                        f.sales_return_type_cd,
                                        CAST(f.sales_return_document_num as STRING),
                                        CAST(f.location_id AS STRING)
                                    )
                    )/tot.total_transactions AS feature_value
            FROM `{}`                              AS f
            INNER JOIN `{}`       AS it  ON f.country_id = it.country_id 
                                                                                   AND f.business_unit_id = it.business_unit_id 
                                                                                   AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}` AS cf ON CASE    
                                                           WHEN TRIM(f.customer_id) = '0' 
                                                             OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                           ELSE TRIM(f.customer_id) 
                                                          END  = cf.customer_id
            INNER JOIN `{}` AS tot ON   CASE  
                                                            WHEN TRIM(f.customer_id) = '0' 
                                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                            ELSE TRIM(f.customer_id) 
                                                        END = tot.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND it.hier_family_cd = '0626'
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0
            GROUP BY 
                feature_name,
                cf.customer_id,
                tot.total_transactions
            ;
        """.format( features_table, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    customers_set_table, 
                    tmp_totals_table, 
                    country_id, 
                    date_from, 
                    date_to)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)

        # End log
        logging.info("Finishing Function: insertFeaturePerTrxRent || Time: {} sec.".format(str(round(time.time()-start))))
        
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerTrxFctr(gcp_OH_tables, country_id, period, date_from, date_to, features_table, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerTrxFctr Function:
    Calculates and inserts per_trx_fctr feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud Plataform BigQuery enviorment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerTrxFctr({}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #customers_set_table = "{}.{}.segmentation_profile_results_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))

        query_string = """
            INSERT INTO `{}`
            SELECT
                cf.customer_id,
                'per_trx_fctr' AS feature_name,
                COUNT(DISTINCT CONCAT(  f.country_id,
                                        f.business_unit_id,
                                        CAST(f.sales_return_dt AS STRING),
                                        f.sales_return_type_cd,
                                        CAST(f.sales_return_document_num as STRING),
                                        CAST(f.location_id AS STRING)
                                    )
                    )/tot.total_transactions AS feature_value
            FROM `{}`                              AS f
            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                                                                                    AND f.business_unit_id = it.business_unit_id                         
                                                                                                    AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{}` AS cf ON CASE    
                                                            WHEN TRIM(f.customer_id) = '0' 
                                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                            ELSE TRIM(f.customer_id) 
                                                        END = cf.customer_id
            INNER JOIN `{}` AS tot ON   CASE  
                                                            WHEN TRIM(f.customer_id) = '0' 
                                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                            ELSE TRIM(f.customer_id) 
                                                        END = tot.customer_id
            WHERE  f.country_id = '{}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd = 'fctr'
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0            
            GROUP BY 
                feature_name,
                cf.customer_id,
                tot.total_transactions
            ;
        """.format(features_table, gcp_OH_tables['sales_table'], gcp_OH_tables['items_table'], customers_set_table, tmp_totals_table, country_id, date_from, date_to)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)

        # End log
        logging.info("Finishing Function: insertFeaturePerTrxFctr || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertAvgTicket(country_id, period, date_from, date_to, features_table, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertAvgTicket Function:
    Calculates and inserts avg_ticket feature for the Classification Algorithm.
    
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        customers_set_table str: name for the table of customers to calculate variables
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud Plataform BigQuery enviorment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertAvgTicket({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #customers_set_table = "{}.{}.segmentation_profile_results_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'avg_ticket' AS feature_name,
                tot.total_sales_amt/tot.total_transactions AS feature_value
            FROM `{}` AS tot
            INNER JOIN `{}` AS cf ON tot.customer_id = cf.customer_id
			;
        """.format(features_table, tmp_totals_table, customers_set_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)

        # End log
        logging.info("Finishing Function: insertAvgTicket || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerTransactionMostTransactedFamily(country_id, period, features_table, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerTransactionMostTransactedFamily Function:
    Calculates and inserts the transaction percentage in the most Transacted Family per Customer.

        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud Plataform BigQuery enviorment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerTransactionMostTransactedFamily({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))

        #prepare table names
        features_table_prev   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT  customer_id,
                    'per_trx_frec_N0' AS feature_name,
                    feature_value 
            FROM (
                SELECT
                    ft.customer_id,
                    feature_value,
                    ROW_NUMBER() OVER(PARTITION BY ft.customer_id ORDER BY ft.feature_value DESC) AS RN
                FROM `{}` AS ft
                INNER JOIN `{}` AS cf ON ft.customer_id = cf.customer_id
                WHERE ft.feature_name like 'per_trx_N0_%'
            )
            WHERE RN = 1
            ;
        """.format(features_table, features_table_prev, customers_set_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)
        
        # End log
        logging.info("Finishing Function: insertFeaturePerTransactionMostTransactedFamily || Time: {} sec.".format(str(round(time.time()-start))))

    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerTransactionMostTransactedSubFamily(country_id, period, features_table, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerTransactionMostTransactedSubFamily Function:
    Calculates and inserts the transaction percentage in the most Transacted Family per Customer.

        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud Plataform BigQuery enviorment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerTransactionMostTransactedSubFamily({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))

        #prepare table names
        features_table_prev   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT  customer_id,
                    'per_trx_frec_N1' AS feature_name,
                    feature_value 
            FROM (
                SELECT
                    ft.customer_id,
                    feature_value,
                    ROW_NUMBER() OVER(PARTITION BY ft.customer_id ORDER BY ft.feature_value DESC) AS RN
                FROM `{}` AS ft
                INNER JOIN `{}` AS cf ON ft.customer_id = cf.customer_id
                WHERE ft.feature_name like 'per_trx_N1_%'
            )
            WHERE RN = 1
            ;
        """.format(features_table, features_table_prev, customers_set_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)
        
        # End log
        logging.info("Finishing Function: insertFeaturePerTransactionMostTransactedSubFamily || Time: {} sec.".format(str(round(time.time()-start))))

    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerUnitsMostTransactedFamily(country_id, period, features_table, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerUnitsMostTransactedFamily Function:
    Calculates and inserts the transaction percentage in the most Transacted Family per Customer.

        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud Plataform BigQuery enviorment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerUnitsMostTransactedFamily({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))

        #prepare table names
        features_table_prev   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT  customer_id,
                    'per_uni_frec_family' AS feature_name,
                    feature_value 
            FROM (
                SELECT
                    ft.customer_id,
                    feature_value,
                    ROW_NUMBER() OVER(PARTITION BY ft.customer_id ORDER BY ft.feature_value DESC) AS RN
                FROM `{}` AS ft
				INNER JOIN `{}` AS cf ON ft.customer_id = cf.customer_id
				WHERE ft.feature_name like 'per_uni_N0_%'
            )
            WHERE RN = 1
            ;
        """.format(features_table, features_table_prev, customers_set_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)
        
        # End log
        logging.info("Finishing Function: insertFeaturePerUnitsMostTransactedFamily || Time: {} sec.".format(str(round(time.time()-start))))

    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerUnitsMostTransactedSubFamily(country_id, period, features_table, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerUnitsMostTransactedSubFamily Function:
    Calculates and inserts the transaction percentage in the most Transacted Family per Customer.

        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud Plataform BigQuery enviorment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerUnitsMostTransactedSubFamily({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))

        #prepare table names
        features_table_prev   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT  customer_id,
                    'per_uni_frec_subfamily' AS feature_name,
                    feature_value 
            FROM (
                SELECT
                    ft.customer_id,
                    feature_value,
                    ROW_NUMBER() OVER(PARTITION BY ft.customer_id ORDER BY ft.feature_value DESC) AS RN
                FROM `{}` AS ft
				INNER JOIN `{}` AS cf ON ft.customer_id = cf.customer_id
				WHERE ft.feature_name like 'per_uni_N1_%'
            )
            WHERE RN = 1
            ;
        """.format(features_table, features_table_prev, customers_set_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)
        
        # End log
        logging.info("Finishing Function: insertFeaturePerUnitsMostTransactedSubFamily || Time: {} sec.".format(str(round(time.time()-start))))

    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeatureTotalUnits(country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeatureTotalUnits Function:
    Calculates and inserts total_stores feature for the Classification Algorithm.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    date_from str: date to start considering data. FORMAT YYYY-MM-DD
    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureTotalUnits({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))    

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'total_units_cnt' AS feature_name,
                tot.total_units_cnt AS feature_value
            FROM `{}` AS tot
            ;
        """.format(features_table, tmp_totals_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeatureTotalUnits || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeatureTotalSales(country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeatureTotalSales Function:
    Calculates and inserts total_stores feature for the Classification Algorithm.
    
    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    date_from str: date to start considering data. FORMAT YYYY-MM-DD
    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureTotalSales({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))    
        
        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'total_sales_amt' AS feature_name,
                tot.total_sales_amt AS feature_value
            FROM `{}` AS tot
            ;
        """.format(features_table, tmp_totals_table)
        
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeatureTotalSales || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeatureTotalTrx(country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeatureTotalTrx Function:
    Calculates and inserts ttoal_trx feature for the Classification Algorithm.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    date_from str: date to start considering data. FORMAT YYYY-MM-DD
    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureTotalTrx({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))    

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'total_transactions' AS feature_name,
                tot.total_transactions AS feature_value
            FROM `{}` AS tot
            ;
        """.format(features_table, tmp_totals_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeatureTotalTrx || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeaturePerDistinctDepartments(country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerDistinctDepartments Function:
    Calculates and inserts per_distinct_department feature for the Classification Algorithm.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    date_from str: date to start considering data. FORMAT YYYY-MM-DD
    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeaturePerDistinctDepartments({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))    

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'per_distinct_department' AS feature_name,
                tot.per_distinct_departments AS feature_value
            FROM `{}` AS tot
            ;
        """.format(features_table, tmp_totals_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeaturePerDistinctDepartments || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def insertFeatureMonthsSinceLastTrx(country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeatureMonthsSinceLastTrx Function:
    Calculates and inserts per_distinct_department feature for the Classification Algorithm.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    date_from str: date to start considering data. FORMAT YYYY-MM-DD
    date_to str: last date of data to consider. FORMAT YYYY-MM-DD
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureMonthsSinceLastTrx({}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, gcp_project, gcp_dataset))    

        #prepare table names
        tmp_totals_table = "{}.{}.segmentation_tmp_totals_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{}`
            SELECT
                tot.customer_id,
                'months_since_last_trx' AS feature_name,
                tot.months_since_last_trx AS feature_value
            FROM `{}` AS tot
            ;
        """.format(features_table, tmp_totals_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: insertFeatureMonthsSinceLastTrx || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise        
        
def insertFeaturePerFamilyCompletness(gcp_OH_tables, country_id, period, date_from, date_to, customers_set_table, gcp_project, gcp_dataset, bqclient):
    """ 
    insertFeaturePerFamilyCompletness Function:
    Calculates and inserts per_family_completeness feature for the Classification Algorithm.

        gcp_OH_tables Dict: Dictionary with the OH table names
        country_id str: Country to process
        period int: Period to process. FORMAT YYYYMM
        date_from str: date to start considering data. FORMAT YYYY-MM-DD
        date_to str: last date of data to consider. FORMAT YYYY-MM-DD
        customers_set_table str: name for the table of customers to calculate variables
        gcp_project str: GCP project to execute the process
        gcp_dataset str: GCP dataset to store the process
        bqclient obj: BigQuery Cliente object needed to connect and execute 
            querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertFeatureTrxHier({}, {}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, 
                                                                                                       country_id, 
                                                                                                       period, 
                                                                                                       date_from, 
                                                                                                       date_to,
                                                                                                       customers_set_table,
                                                                                                       gcp_project,
                                                                                                       gcp_dataset))

        #prepare table names
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        query_string = """
            INSERT INTO `{0:s}`
            SELECT
                cf.customer_id,
                CONCAT('per_family_completeness_', TRIM(it.hier_family_cd)) AS feature_name,
                COUNT(DISTINCT TRIM(it.hier_subfamily_cd))/itr.subfamilies AS feature_value
            FROM `{1:s}` AS f
            INNER JOIN `{2:s}` AS it  ON f.country_id = it.country_id 
                                                                                   AND f.business_unit_id = it.business_unit_id 
                                                                                   AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            INNER JOIN `{3:s}` AS cf ON CASE
                                                                                        WHEN TRIM(f.customer_id) = '0' 
                                                                                          OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                                                                        ELSE TRIM(f.customer_id) 
                                                                                    END = cf.customer_id
            INNER JOIN (
                    SELECT  country_id,
                            business_unit_id, 
                            TRIM(hier_family_cd) as hier_family_cd, 
                            COUNT(DISTINCT TRIM(hier_subfamily_cd)) as subfamilies
                    FROM `{2:s}`
                    WHERE country_id = '{4:s}'
                    AND   business_unit_id = 'ENT'
                    AND   SAFE_CAST(item_id AS INT64) IS NOT NULL
                    GROUP BY 1, 2, 3
            )AS itr ON it.country_id = itr.country_id 
                    AND it.business_unit_id = itr.business_unit_id 
                    AND TRIM(it.hier_family_cd) = TRIM(itr.hier_family_cd)
            WHERE  f.country_id = '{4:s}'
            AND f.business_unit_id = 'ENT'
            AND f.sales_return_dt >= '{5:s}' 
            AND f.sales_return_dt < '{6:s}'
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0
            AND it.hier_family_cd IS NOT NULL
            AND it.hier_subfamily_cd IS NOT NULL
            GROUP BY 
                feature_name,
                cf.customer_id,
                itr.subfamilies
            ;
        """.format(features_table, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    customers_set_table, 
                    country_id, 
                    date_from, 
                    date_to)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()

        # End log
        logging.info("Finishing Function: insertFeatureTrxHier || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

###############################################################################
#         					 Create All Features         					 #
###############################################################################
#Executes all previous Functions
def insertallFeatures(gcp_OH_tables, country_id, period, process_dates, customers_set_table, training, gcp_project, gcp_dataset, bqclient):
    """ 
    insertallFeatures Function:
    Calculates and inserts all features needed for the Classification Algorithm.

	    country_id str: Country to process
	    period int: Period to process. FORMAT YYYYMM
	    process_dates Dict: dates needed for the process. FORMAT YYYY-MM-DD
	    standardize Bool: if it is true, all collaborations that are not 
	                      will be taken to values between 0 and 1.
	    customers_set_table str: name for the table of customers to calculate variables
	    gcp_project str: GCP project to execute the process
	    gcp_dataset str: GCP dataset to store the process
	    bqclient obj: BigQuery Cliente object needed to connect and execute 
	        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: insertallFeatures({}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, process_dates, customers_set_table, gcp_project, gcp_dataset))    

        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))

        # Create total temporal table
        createTemporalTotalsTable( gcp_OH_tables,
                                    country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    customers_set_table, 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)

        ###########################################################################
        #                      	  LVL 0 EXTRA FEATURES                      	  #
        ###########################################################################
        # The first feature to insert is per_trx_store since is needed to insert the second feature per_trx_best_store
        # and since the code uses the same table is cheaper for GCP to process less rows:
        #   -per_trx_store_location_id
        feat_level = "CONCAT('per_trx_store_',LPAD(CAST(f.location_id AS STRING), 3, '0'))"
        insertFeaturesPerTransactions(  gcp_OH_tables,
                                        country_id, 
                                        period, 
                                        process_dates['date_from'], 
                                        process_dates['date_to'], 
                                        feat_level, 
                                        customers_set_table, 
                                        gcp_project, 
                                        gcp_dataset,
                                        bqclient )
        #percentage of transactions in the store more visited
        insertFeaturePerTransactionMostFrecuentStore(country_id, period, gcp_project, gcp_dataset, bqclient)
        
        #Features from table Totals
        insertFeaturesSkuPro(gcp_OH_tables, country_id, period, process_dates['date_from'], process_dates['date_to'], customers_set_table, gcp_project, gcp_dataset, bqclient)
        insertFeaturePerDistinctDepartments(country_id, period, process_dates['date_from'], process_dates['date_to'], gcp_project, gcp_dataset, bqclient)
        insertFeatureMonthsSinceLastTrx(country_id, period, process_dates['date_from'], process_dates['date_to'], gcp_project, gcp_dataset, bqclient)
        insertFeaturePerFamilyCompletness(gcp_OH_tables, country_id, period, process_dates['date_from'], process_dates['date_to'],  customers_set_table, gcp_project, gcp_dataset, bqclient)

        # Insert transaction percentage Features at different feature levels:
        #   -per_trx_channel_sales_flg
        #   -per_trx_hour_24hours
        #   -per_trx_store_location_id
        #   -per_trx_weekday_7days
        feature_levels = [  
                            "CONCAT('per_trx_hour_',LPAD(CAST(EXTRACT(HOUR FROM SAFE_CAST(f.transaction_dttm AS DATETIME)) AS STRING), 2, '0'))",
                            "CONCAT('per_trx_weekday_',LPAD(CAST(EXTRACT(DAYOFWEEK FROM SAFE_CAST(f.sales_return_dt AS DATETIME)) AS STRING), 2, '0'))",
                            "CONCAT('per_trx_channel_',IF(TRIM(f.sub_channel_level1_sales_flg)='SR', 'SR', 'NSR'))" 
                        ]
        
        for feature_level in feature_levels:
            insertFeaturesPerTransactions(  gcp_OH_tables,
                                            country_id, 
                                            period, 
                                            process_dates['date_from'], 
                                            process_dates['date_to'], 
                                            feature_level, 
                                            customers_set_table, 
                                            gcp_project, 
                                            gcp_dataset,
                                            bqclient )
        
        # Insert transactions features for each CLACOM Level in the hierarchy_levels List
        # Features to calculate:
        # -per_trx_hier
        # -per_uni_hier
        # -trx_hier
        hierarchy_levels = [("N0","hier_family_cd"),
                            ("N2","hier_group_cd"),
                            ("N1","hier_subfamily_cd")]

        for hier_level in hierarchy_levels:
            #   -per_trx_hier
            insertFeaturePerTrxHier(gcp_OH_tables, 
                                    country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    "CONCAT('per_trx_{}_',trim(it.{}))".format(hier_level[0], hier_level[1]), 
                                    customers_set_table, 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
            
            #   -per_uni_hier
            insertFeaturePerUniHier(gcp_OH_tables, 
                                    country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    "CONCAT('per_uni_{}_',trim(it.{}))".format(hier_level[0], hier_level[1]), 
                                    customers_set_table, 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
            
        #   -trx_hier (SubFamily only)
        insertFeatureTrxHier(   gcp_OH_tables,
                                country_id, 
                                period, 
                                process_dates['date_from'], 
                                process_dates['date_to'], 
                                "CONCAT('trx_N1_',trim(it.hier_subfamily_cd))", 
                                customers_set_table, 
                                gcp_project, 
                                gcp_dataset,
                                bqclient)
        
        # Other Insert: 
        #   -avg_units_trx
        insertAvgUnitsTrx(  country_id, 
                            period, 
                            process_dates['date_from'], 
                            process_dates['date_to'], 
                            gcp_project, 
                            gcp_dataset,
                            bqclient)
        
        #   -per_trx_ntcr
        insertFeaturePerTrxNtcr(    gcp_OH_tables,
                                    country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    customers_set_table, 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
        
        #   -per_sales_ntcr
        insertFeaturePerSalesNtcr(  gcp_OH_tables,
                                    country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    customers_set_table, 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
        
        #   -set_diversity
        insertFeatureSetDiversity(  country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
        
        #   -skus_subfamily_hier_subfamily_cd
        insertFeatureSkusSubfamily( gcp_OH_tables,
                                    country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    "CONCAT('skus_N1_',TRIM(it.hier_subfamily_cd))", 
                                    customers_set_table, 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
        
        #   -total_stores
        insertFeatureTotalStores(   country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
        
        #   -total_units
        insertFeatureTotalUnits(   country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
        #   -total_sales
        insertFeatureTotalSales(   country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)
        #   -total_trx
        insertFeatureTotalTrx(   country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    gcp_project, 
                                    gcp_dataset,
                                    bqclient)        

        ###########################################################################
        #                      	  LVL 1 EXTRA FEATURES                      	  #
        ###########################################################################
        insertFeaturePerTrxHier( gcp_OH_tables, 
		                         country_id, 
		                         period, 
		                         process_dates['date_from'], 
		                         process_dates['date_to'], 
		                         "CONCAT('per_trx_ND_',trim(it.hier_department_cd))", 
		                         customers_set_table, 
		                         gcp_project, 
		                         gcp_dataset,
		                         bqclient)

        ###########################################################################
        #                      	  LVL 2 EXTRA FEATURES                      	  #
        ###########################################################################
        insertAvgTicket(country_id, 
		                    period, 
		                    process_dates['date_from'], 
		                    process_dates['date_to'], 
		                    features_table,
							customers_set_table,
                            gcp_project, 
		                    gcp_dataset, 
		                    bqclient)
        
        insertFeaturePerTrxRent(gcp_OH_tables,
				                    country_id,
				                    period,
				                    process_dates['date_from'], 
				                    process_dates['date_to'], 
									features_table,
									customers_set_table,
				                    gcp_project, 
				                    gcp_dataset, 
				                    bqclient)

        insertFeaturePerTrxFctr(gcp_OH_tables,
				                    country_id,
				                    period,
				                    process_dates['date_from'],
				                    process_dates['date_to'],
				                    features_table,
				                    customers_set_table,
				                    gcp_project,
				                    gcp_dataset,
				                    bqclient)

        insertFeaturePerTransactionMostTransactedFamily(country_id,
				                                        period,
				                                        features_table,
				                                        customers_set_table,
				                                        gcp_project,
				                                        gcp_dataset,
				                                        bqclient)

        insertFeaturePerTransactionMostTransactedSubFamily( country_id,
				                                            period,
				                                            features_table,
				                                            customers_set_table,
				                                            gcp_project,
				                                            gcp_dataset,
				                                            bqclient)

        insertFeaturePerUnitsMostTransactedFamily(  country_id,
				                                    period,
				                                    features_table,
				                                    customers_set_table,
				                                    gcp_project,
				                                    gcp_dataset,
				                                    bqclient)

        insertFeaturePerUnitsMostTransactedSubFamily(   country_id,
				                                        period,
				                                        features_table,
				                                        customers_set_table,
				                                        gcp_project,
				                                        gcp_dataset,
				                                        bqclient)
        # End log
        logging.info("Finishing Function: insertallFeatures || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

###############################################################################
#                             Create Feature Sets                             #
###############################################################################
def createFeaturesSet(country_id, period, gcp_project, gcp_dataset, bqclient, training, customers_set_table, features_list, standardize, std_feature_names, std_type='Z', seglevel='segmentation', drop_outliers=False):
    """ 
    createFeaturesSet Function:
    Generates standardized and normalized values for the feature_names given .

	    country_id str: Country to process
		period int: Period to process. FORMAT YYYYMM
		gcp_project str: GCP project to execute the process
		gcp_dataset str: GCP dataset to store the process
		bqclient obj: BigQuery Cliente object needed to connect and execute 
		    querys and statements in Google Cloud platform BigQuery environment
		training bool: Define if thre is a training process going on or just a scoring
		customers_set_table str: table with all the customers to work with
		feature_names List: List of "like conditions" to filter the names of the Features
		standardize bool: True if Stadarization is needed
		std_feature_names List: List of "like conditions" of the names of the Features to standardize 
		std_type str: Type of Stadarization to use 'Z' or 'MinMax'
		seglevel str: Level of process for example 'segmentation'
        drop_outliers bool: Indicates if outliers must be dropped. Default: False
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createFeaturesSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset, std_type, seglevel))    

        #prepare table names
        std_table       = "{}.{}.{}_standardization_values_{}".format(gcp_project, gcp_dataset, seglevel, country_id)
        featset_table   = "{}.{}.{}_features_set_{}{}".format(gcp_project, gcp_dataset, seglevel, country_id, str(period))
        features_table  = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        out_table       = "{}.{}.{}_training_customers_noout_{}{}".format(gcp_project, gcp_dataset, seglevel, country_id, str(period))

        #Preapre Standardization function:
        std_func = ""
        if std_type == 'Z':
            std_func = "((ft.feature_value-B.avg_feature)/B.std_dev_feature)"
        else:
            std_func = "((ft.feature_value-B.min_value)/(B.max_value-B.min_value))"

        logging.info("createFeaturesSet: Stanrdize Function {}".format(std_type))                
        logging.info("createFeaturesSet: Stanrdize Function {}".format(std_func)) 
        
        #Prepare conditions with variables to include:
        conditions_f = "WHERE"
        first = True
        for condition in features_list:
            if first:
                conditions_f = conditions_f + " ft.feature_name like '" + condition + "'"
                first = False 
            else:
                conditions_f = conditions_f + " OR ft.feature_name like '" + condition + "'"
        
        
		#Prepare conditions with variables to standardize:
        conditions = "WHERE"
        first = True
        for condition in std_feature_names:
            if first:
                conditions = conditions + " ft.feature_name like '" + condition + "'"
                first = False 
            else:
                conditions = conditions + " OR ft.feature_name like '" + condition + "'"

        logging.info("createFeaturesSet: Features to use: {}".format(conditions_f))
        logging.info("createFeaturesSet: Features to standardize: {}".format(conditions))
        
        if training and standardize:
        	#If excecuting a training exercise and standardization is needed
        	#then Create a new Standardization Parameters Table:

            #DROP PARAMETERS TABLE IF EXISTS
            query_drop = "DROP TABLE IF EXISTS `{}`;".format(std_table)
            logging.info("SQL: " + query_drop)
            bqclient.query(query_drop).result()

            #CREATE PARAMETERS TABLE
            query_create = """
                CREATE TABLE `{}` AS
                SELECT  {} as period, 
                        '{}' as std_type,
                        feature_name,
                        AVG(feature_value) as avg_feature,
                        STDDEV_POP(feature_value) as std_dev_feature,
                        MIN(feature_value) AS min_value,
                        MAX(feature_value) AS max_value
                FROM `{}` ft
                INNER JOIN `{}` cs ON ft.customer_id = cs.customer_id
                {}
                GROUP BY feature_name
                HAVING STDDEV_POP(feature_value) <> 0;
            """.format(	std_table, 
            			period, 
                        std_type,
            			features_table, 
            			customers_set_table if not(drop_outliers) else out_table, 
            			conditions)
            logging.info("SQL: "+query_create)
            bqclient.query(query_create).result()
                
	        #DROP TABLE FEATURES SET TABLE
            query_drop = "DROP TABLE IF EXISTS `{}`;".format(featset_table)
            logging.info("SQL: " + query_drop)
            bqclient.query(query_drop).result()
	                
            #CREATE NEW FEATURES SET TABLE
            query_create = """
                CREATE TABLE `{}` AS
                SELECT ft.customer_id,
                       ft.feature_name,
                       CASE 
                           WHEN B.feature_name IS NULL 
                           OR B.std_dev_feature = 0 THEN ft.feature_value
                           ELSE {}
                       END feature_value
                FROM `{}` ft
                INNER JOIN `{}` cs ON ft.customer_id = cs.customer_id
                LEFT JOIN `{}` B ON ft.feature_name = B.feature_name
                {}
            ;
            """.format(featset_table, 
                       std_func,
                       features_table,
                       customers_set_table,
                       std_table, 
                       conditions_f)
            logging.info("SQL: " + query_create)
            bqclient.query(query_create).result()

        elif standardize and not(training):
            #If excecuting a non-traning exercise (Scoring) then 
            #use the last Parameters table to create the FeaturesSet:

            #DROP TABLE FEATURES SET TABLE
            query_drop = "DROP TABLE IF EXISTS `{}`;".format(featset_table)
            logging.info("SQL: " + query_drop)
            bqclient.query(query_drop).result()
                    
            #CREATE NEW FEATURES SET TABLE
            query_create = """
                CREATE TABLE `{}` AS
                SELECT ft.customer_id,
                       ft.feature_name,
                       CASE 
                           WHEN B.feature_name IS NULL 
                           		OR B.std_dev_feature = 0 THEN ft.feature_value
                           ELSE {}
                       END feature_value
                FROM `{}` ft
                INNER JOIN `{}` cs ON ft.customer_id = cs.customer_id
                LEFT JOIN `{}` B ON ft.feature_name = B.feature_name
                {}
            ;
            """.format(featset_table, std_func, features_table, customers_set_table, std_table, conditions_f)
            logging.info("SQL: " + query_create)
            bqclient.query(query_create).result()
        
        else:
            #Finally if there is no training nor standardization needed 
            # just create the table with the features needed for the customer_set 
            
            #DROP TABLE OG FEATURES TABLE
            query_drop = "DROP TABLE IF EXISTS `{}`;".format(featset_table)
            logging.info("SQL: " + query_drop)
            bqclient.query(query_drop).result()
                    
            #CREATE NEW FEATURES TABLE
            query_create = """
                CREATE TABLE `{}` AS
                SELECT ft.customer_id,
                       ft.feature_name,
                       ft.feature_value
                FROM `{}` ft
                INNER JOIN `{}` cs ON ft.customer_id = cs.customer_id
                {}
            ;
            """.format(featset_table, features_table, customers_set_table, conditions_f)
            logging.info("SQL: " + query_create)
            bqclient.query(query_create).result()

        # End log
        logging.info("Finishing Function: createFeaturesSet || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
        
def createFeaturesSetOutliers(country_id, period, gcp_project, gcp_dataset, bqclient, training, customers_set_table, features_list, standardize, std_feature_names, std_type='Z', seglevel='segmentation', drop_outliers=False):
    """ 
    createFeaturesSet Function:
    Generates standardized and normalized values for the feature_names given .

	    country_id str: Country to process
		period int: Period to process. FORMAT YYYYMM
		gcp_project str: GCP project to execute the process
		gcp_dataset str: GCP dataset to store the process
		bqclient obj: BigQuery Cliente object needed to connect and execute 
		    querys and statements in Google Cloud platform BigQuery environment
		training bool: Define if thre is a training process going on or just a scoring
		customers_set_table str: table with all the customers to work with
		feature_names List: List of "like conditions" to filter the names of the Features
		standardize bool: True if Stadarization is needed
		std_feature_names List: List of "like conditions" of the names of the Features to standardize 
		std_type str: Type of Stadarization to use 'Z' or 'MinMax'
		seglevel str: Level of process for example 'segmentation'
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createFeaturesSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset, std_type, seglevel))    

        #prepare table names
        std_table       = "{}.{}.{}_standardization_values_{}".format(gcp_project, gcp_dataset, seglevel, country_id)
        featset_table   = "{}.{}.{}_features_set_{}{}".format(gcp_project, gcp_dataset, seglevel, country_id, str(period))
        features_table  = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        out_table       = "{}.{}.{}_training_customers_noout_{}{}".format(gcp_project, gcp_dataset, seglevel, country_id, str(period))

        #Preapre Standardization function:
        std_func = ""
        if std_type == 'Z':
            std_func = "((ft.feature_value-B.avg_feature)/B.std_dev_feature)"
        else:
            std_func = "((ft.feature_value-B.min_value)/(B.max_value-B.min_value))"

        logging.info("createFeaturesSet: Stanrdize Function {}".format(std_type))                
        logging.info("createFeaturesSet: Stanrdize Function {}".format(std_func)) 
        
        #Prepare conditions with variables to include:
        conditions_f = "WHERE"
        first = True
        for condition in features_list:
            if first:
                conditions_f = conditions_f + " ft.feature_name like '" + condition + "'"
                first = False 
            else:
                conditions_f = conditions_f + " OR ft.feature_name like '" + condition + "'"
        
        
		#Prepare conditions with variables to standardize:
        conditions = "WHERE"
        first = True
        for condition in std_feature_names:
            if first:
                conditions = conditions + " ft.feature_name like '" + condition + "'"
                first = False 
            else:
                conditions = conditions + " OR ft.feature_name like '" + condition + "'"

        logging.info("createFeaturesSet: Features to use: {}".format(conditions_f))
        logging.info("createFeaturesSet: Features to standardize: {}".format(conditions))
        
        if training and standardize:
        	#If excecuting a training exercise and standardization is needed
        	#then Create a new Standardization Parameters Table:

            #DROP PARAMETERS TABLE IF EXISTS
            query_drop = "DROP TABLE IF EXISTS `{}`;".format(std_table)
            logging.info("SQL: " + query_drop)
            bqclient.query(query_drop).result()

            #CREATE PARAMETERS TABLE
            query_create = """
                CREATE TABLE `{}` AS
                SELECT  {} as period, 
                        feature_name,
                        AVG(feature_value) as avg_feature,
                        STDDEV_POP(feature_value) as std_dev_feature,
                        MIN(feature_value) AS min_value,
                        MAX(feature_value) AS max_value
                FROM `{}` ft
                INNER JOIN `{}` cs ON ft.customer_id = cs.customer_id
                {}
                GROUP BY feature_name
                HAVING STDDEV_POP(feature_value) <> 0;
            """.format(	std_table, 
            			period, 
            			features_table, 
            			customers_set_table if not(drop_outliers) else out_table, 
            			conditions)
            logging.info("SQL: "+query_create)
            bqclient.query(query_create).result()
                
	        #DROP TABLE FEATURES SET TABLE
            query_drop = "DROP TABLE IF EXISTS `{}`;".format(featset_table)
            logging.info("SQL: " + query_drop)
            bqclient.query(query_drop).result()
	                
            #CREATE NEW FEATURES SET TABLE
            query_create = """
                CREATE TABLE `{}` AS
                SELECT ft.customer_id,
                       ft.feature_name,
                       CASE 
                           WHEN B.feature_name IS NULL 
                           OR B.std_dev_feature = 0 THEN ft.feature_value
                           ELSE {}
                       END feature_value
                FROM `{}` ft
                INNER JOIN `{}` cs ON ft.customer_id = cs.customer_id
                LEFT JOIN `{}` B ON ft.feature_name = B.feature_name
                {}
            ;
            """.format(featset_table, 
                       std_func,
                       features_table,
                       customers_set_table,
                       std_table, 
                       conditions_f)
            logging.info("SQL: " + query_create)
            bqclient.query(query_create).result()

        elif standardize:
            #If excecuting a non-traning exercise (Scoring) then 
            #use the last Parameters table to create the FeaturesSet:

            #DROP TABLE FEATURES SET TABLE
            query_drop = "DROP TABLE IF EXISTS `{}`;".format(featset_table)
            logging.info("SQL: " + query_drop)
            bqclient.query(query_drop).result()
                    
            #CREATE NEW FEATURES SET TABLE
            query_create = """
                CREATE TABLE `{}` AS
                SELECT ft.customer_id,
                       ft.feature_name,
                       CASE 
                           WHEN B.feature_name IS NULL 
                           		OR B.std_dev_feature = 0 THEN ft.feature_value
                           ELSE {}
                       END feature_value
                FROM `{}` ft
                INNER JOIN `{}` cs ON ft.customer_id = cs.customer_id
                LEFT JOIN `{}` B ON ft.feature_name = B.feature_name
                {}
            ;
            """.format(featset_table, std_func, features_table, customers_set_table, std_table, conditions_f)
            logging.info("SQL: " + query_create)
            bqclient.query(query_create).result()
        
        else:
            #Finally if there is no training nor standardization needed 
            # just create the table with the features needed for the customer_set 
            
            #DROP TABLE OG FEATURES TABLE
            query_drop = "DROP TABLE IF EXISTS `{}`;".format(featset_table)
            logging.info("SQL: " + query_drop)
            bqclient.query(query_drop).result()
                    
            #CREATE NEW FEATURES TABLE
            query_create = """
                CREATE TABLE `{}` AS
                SELECT ft.customer_id,
                       ft.feature_name,
                       ft.feature_value
                FROM `{}` ft
                INNER JOIN `{}` cs ON ft.customer_id = cs.customer_id
                {}
            ;
            """.format(featset_table, features_table, customers_set_table, conditions_f)
            logging.info("SQL: " + query_create)
            bqclient.query(query_create).result()

        # End log
        logging.info("Finishing Function: createFeaturesSet || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
        

        