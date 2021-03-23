#customers
"""  
Customer Classification: segmentcustomers
@autor: Cristopher Manriquez
@description: Functions involving the creation and management of features and 
variables needed across for the  Customer classification process

"""
###############################################################################
#                           Libraries and Imports                             #
###############################################################################
import logging
import time

###############################################################################
#                          Define Customers Functions                         #
###############################################################################
def createTemporalVEECustomers(gcp_OH_tables, country_id, period, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    createTemporalVEECustomers Function:
    Creates a table with the list of Customer with at least one transaction in the VE channel.

    gcp_OH_tables Dict: Dictionary with the OH table names
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
        logging.info("Start Function: createTemporalVEECustomers({}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_customers_vehist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()

        query_string = """
            CREATE TABLE `{}` AS
            SELECT DISTINCT
                    CASE
                        WHEN TRIM(f.customer_id) = '0' 
                        OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id)
                        ELSE TRIM(f.customer_id) 
                    END AS customer_id
            FROM `{}` AS f
            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                          AND f.business_unit_id = it.business_unit_id                         
                                          AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            LEFT JOIN `{}` AS bl ON f.country_id = bl.country_id 
                            AND CASE 
                                    WHEN TRIM(f.customer_id) = '0' 
                                    OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                    ELSE TRIM(f.customer_id)  
                                END = TRIM(bl.customer_id)
            LEFT JOIN `{}` AS ge ON f.country_id = ge.country_id 
                            AND CASE 
                                WHEN TRIM(f.customer_id) = '0' 
                                OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                ELSE TRIM(f.customer_id)  
                            END = TRIM(ge.customer_id)                            
            WHERE f.country_id = '{}'
            AND f.business_unit_id='ENT'
            AND f.sales_return_dt >= '2016-01-01'
            AND f.sales_return_dt < '{}'
            AND f.business_area_sales_flg = 'VE'
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND bl.customer_id IS NULL
            AND (ge.customer_id IS NULL OR ge.tramo_ventas < {})
            ;
        """.format( table_name, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    gcp_OH_tables['black_list_table'], 
                    gcp_OH_tables['ggee'], 
                    country_id, 
                    date_to,
                    str(gcp_OH_tables['ggee_sales_tranche'])
                    )

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: createTemporalVEECustomers || Time: {} sec.".format(str(round(time.time()-start)))) 
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def createTemporalGGEECustomers(gcp_OH_tables, country_id, period, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    createTemporalGGEECustomers Function:
    Creates a table with the list of Customer with at least 1 transaction and a sales tranche of 4 or more.

    gcp_OH_tables Dict: Dictionary with the OH table names
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
        logging.info("Start Function: createTemporalGGEECustomers({}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_customers_ggeehist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()

        query_string = """
            CREATE TABLE `{}` AS
            SELECT DISTINCT
                    CASE
                        WHEN TRIM(f.customer_id) = '0' 
                        OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id)
                        ELSE TRIM(f.customer_id) 
                    END AS customer_id,
                    tramo_ventas as sales_tranche
            FROM `{}` AS f
            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                          AND f.business_unit_id = it.business_unit_id                         
                                          AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            LEFT JOIN `{}` AS bl ON f.country_id = bl.country_id 
                                AND CASE 
                                        WHEN TRIM(f.customer_id) = '0' 
                                        OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                        ELSE TRIM(f.customer_id)  
                                    END = TRIM(bl.customer_id)
            INNER JOIN `{}` AS ge ON f.country_id = ge.country_id 
                                 AND CASE 
                                         WHEN TRIM(f.customer_id) = '0' 
                                         OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                         ELSE TRIM(f.customer_id)  
                                     END = TRIM(ge.customer_id)                            
            WHERE f.country_id = '{}'
            AND f.business_unit_id='ENT'
            AND f.sales_return_dt >= '2016-01-01'
            AND f.sales_return_dt < '{}'
            AND f.st_extended_net_amt > 0 
            AND f.sales_return_type_cd IN ('blta', 'fctr')
            AND f.unit_cnt > 0
            AND bl.customer_id IS NULL
            ;
        """.format( table_name, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    gcp_OH_tables['black_list_table'], 
                    gcp_OH_tables['ggee'], 
                    country_id, 
                    date_to
                  )

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: createTemporalGGEECustomers || Time: {} sec.".format(str(round(time.time()-start)))) 
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def createTemporalFCTRCustomers(gcp_OH_tables, country_id, period, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    createTemporalFCTRCustomers Function:
    Creates a table with the list of Customer with at least one transaction with document ['ftrc'].

    gcp_OH_tables Dict: Dictionary with the OH table names
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
        logging.info("Start Function: createTemporalFCTRCustomers({}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_customers_fctrhist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        ve_table = "{}.{}.segmentation_customers_vehist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        if gcp_OH_tables['target_table'] == None:
            query_string = """
                CREATE TABLE `{}` AS
                SELECT DISTINCT
                       CASE 
                            WHEN TRIM(f.customer_id) = '0' 
                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                            ELSE TRIM(f.customer_id)  
                       END AS customer_id
                FROM `{}` AS f
                INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                                AND f.business_unit_id = it.business_unit_id                         
                                                AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
                LEFT JOIN `{}` AS bl ON f.country_id = bl.country_id 
                                     AND CASE 
                                            WHEN TRIM(f.customer_id) = '0' 
                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                            ELSE TRIM(f.customer_id)
                                        END = TRIM(bl.customer_id)
                LEFT JOIN `{}` AS ge ON f.country_id = ge.country_id 
                                     AND CASE 
                                            WHEN TRIM(f.customer_id) = '0' 
                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                            ELSE TRIM(f.customer_id)
                                        END = TRIM(ge.customer_id)                        
                LEFT JOIN `{}` AS h  ON CASE 
                                            WHEN TRIM(f.customer_id) = '0' 
                                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                            ELSE TRIM(f.customer_id)
                                        END = h.customer_id
                WHERE f.country_id = '{}'
                AND f.business_unit_id='ENT'
                AND f.sales_return_dt >= '2016-01-01'
                AND f.sales_return_dt < '{}'
                AND f.sales_return_type_cd = 'fctr'
                AND f.st_extended_net_amt > 0 
                AND f.unit_cnt > 0
                AND bl.customer_id IS NULL
                AND (ge.customer_id IS NULL OR ge.tramo_ventas < {})
                AND h.customer_id IS NULL
                ; 
            """.format( table_name, 
                        gcp_OH_tables['sales_table'], 
                        gcp_OH_tables['items_table'], 
                        gcp_OH_tables['black_list_table'],
                        gcp_OH_tables['ggee'], 
                        ve_table, 
                        country_id, 
                        date_to,
                        str(gcp_OH_tables['ggee_sales_tranche']), 
                        )
        else:
            query_string = """
                CREATE TABLE `{}` AS
                SELECT t.customer_id
                FROM `{}` AS t
                LEFT JOIN `{}` AS bl ON t.country_id = bl.country_id 
                                     AND TRIM(t.customer_id) = TRIM(bl.customer_id)
                LEFT JOIN `{}` AS ge ON t.country_id = ge.country_id 
                                     AND TRIM(t.customer_id) = TRIM(ge.customer_id)
                LEFT JOIN `{}` AS ve  ON TRIM(t.customer_id) = ve.customer_id
                WHERE bl.customer_id IS NULL
                AND ve.customer_id IS NULL
                AND t.country_id = '{}'
                AND (ge.customer_id IS NULL OR ge.tramo_ventas < {})
                ; 
            """.format( table_name, 
                        gcp_OH_tables['target_table'], 
                        gcp_OH_tables['black_list_table'], 
                        gcp_OH_tables['ggee'], 
                        ve_table, 
                        country_id,
                        str(gcp_OH_tables['ggee_sales_tranche'])
                )
        
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: createTemporalFCTRCustomers || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise  

def createTemporalBLTACustomers(gcp_OH_tables, country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """ 
    createTemporalBLTACustomers Function:
    Creates a table with the list of Customers who have never bought with Documents 'fctr' or in the VE Channel

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
        logging.info("Start Function: createTemporalBLTACustomers({}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_customers_blta_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        ve_table = "{}.{}.segmentation_customers_vehist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        fctr_table ="{}.{}.segmentation_customers_fctrhist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))

        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        query_string = """
            CREATE TABLE `{}` AS
            SELECT DISTINCT
                        CASE 
                            WHEN TRIM(f.customer_id) = '0' 
                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                            ELSE TRIM(f.customer_id)
                        END AS customer_id
            FROM `{}` AS f
            INNER JOIN `{}`       AS it   ON f.country_id = it.country_id 
                                          AND f.business_unit_id = it.business_unit_id                         
                                          AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
            LEFT JOIN `{}` h  ON CASE 
                            WHEN TRIM(f.customer_id) = '0' 
                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                            ELSE TRIM(f.customer_id)
                        END = h.customer_id
            LEFT JOIN `{}` e  ON CASE 
                            WHEN TRIM(f.customer_id) = '0' 
                            OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                            ELSE TRIM(f.customer_id)
                        END = e.customer_id
            LEFT JOIN `{}` AS bl ON f.country_id = bl.country_id 
                                 AND CASE 
                                        WHEN TRIM(f.customer_id) = '0' 
                                        OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                        ELSE TRIM(f.customer_id)
                                    END = TRIM(bl.customer_id)
            LEFT JOIN `{}` AS ge ON f.country_id = ge.country_id 
                                AND CASE 
                                        WHEN TRIM(f.customer_id) = '0' 
                                        OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                        ELSE TRIM(f.customer_id)
                                    END = TRIM(ge.customer_id)                        
            WHERE f.country_id = '{}'
            AND f.business_unit_id='ENT'
            AND f.sales_return_dt >= '{}' 
            AND f.sales_return_dt < '{}'
            AND f.sales_return_type_cd = 'blta'
            AND f.st_extended_net_amt > 0 
            AND f.unit_cnt > 0
            AND h.customer_id IS NULL 
            AND e.customer_id IS NULL 
            AND bl.customer_id IS NULL
            AND (ge.customer_id IS NULL OR ge.tramo_ventas < {})            
            ; 
        """.format( table_name, 
                    gcp_OH_tables['sales_table'], 
                    gcp_OH_tables['items_table'], 
                    ve_table, 
                    fctr_table, 
                    gcp_OH_tables['black_list_table'], 
                    gcp_OH_tables['ggee'], 
                    country_id, 
                    date_from, 
                    date_to,
                    str(gcp_OH_tables['ggee_sales_tranche'])
                    )

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: createTemporalBLTACustomers || Time: {} sec.".format(str(round(time.time()-start))))    
        return table_name
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

###############################################################################
#                     Training Sets Functions Definitions                     #
###############################################################################
def createTemporalAllCustomerSet(country_id, period, gcp_project, gcp_dataset, bqclient):
    """
    createTemporalAllCustomerSet Function:
    Joins All Fctr Customers and Blta Customers to calculate features.

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
        logging.info("Start Function: createTemporalAllCustomerSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_all_customers_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        fctr_table = "{}.{}.segmentation_customers_fctrhist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        blta_table = "{}.{}.segmentation_customers_blta_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))

        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        query_string = """
            CREATE TABLE `{}` AS
            SELECT 
                customer_id, 1 as target
            FROM `{}`
            UNION ALL 
            SELECT 
                customer_id, 0 as target
            FROM `{}`
            ;
        """.format(table_name, fctr_table, blta_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: createTemporalAllCustomerSet || Time: {} sec.".format(str(round(time.time()-start))))    
        return table_name
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

        
def createTemporalAllCustomerSetVE(country_id, period, gcp_project, gcp_dataset, bqclient):
    """
    createTemporalAllCustomerSetVE Function:
    Joins All Fctr Customers and Blta Customers to calculate features.

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
        logging.info("Start Function: createTemporalAllCustomerSetVE({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_all_customers_ve_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        fctr_table = "{}.{}.segmentation_customers_fctrhist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        ggee_table = "{}.{}.segmentation_customers_ggeehist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        blta_table = "{}.{}.segmentation_customers_blta_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        ve_table = "{}.{}.segmentation_customers_vehist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))

        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        query_string = """
            CREATE TABLE `{}` AS
            SELECT DISTINCT customer_id 
            FROM (
                SELECT 
                    customer_id, 1 as target
                FROM `{}`
                UNION ALL 
                SELECT 
                    customer_id, 0 as target
                FROM `{}`
                UNION ALL 
                SELECT 
                    customer_id, 0 as target
                FROM `{}`
                UNION ALL 
                SELECT 
                    customer_id, 0 as target
                FROM `{}`
            )
            ;
        """.format(table_name, 
                   fctr_table, 
                   blta_table, 
                   ve_table,
                   ggee_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: createTemporalAllCustomerSetVE || Time: {} sec.".format(str(round(time.time()-start))))    
        return table_name
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

def createTemporalPROCustomerData(gcp_OH_tables, country_id, period, date_from, date_to, gcp_project, gcp_dataset, bqclient):
    """
    createTemporalPROCustomer Function:
    Creates a table with Customers Data to consider typeB in the training exercise.

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
        logging.info("Start Function: createTemporalPROCustomerData({}, {}, {}, {}, {}, {}, {})".format(gcp_OH_tables, country_id, period, date_from, date_to, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_customers_fctrdata_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        ve_table = "{}.{}.segmentation_customers_vehist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        fctr_table = "{}.{}.segmentation_customers_fctrhist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}` AS
            SELECT 
                t1.customer_id,
                SUM(t1.total_transactions) AS total_transactions,
                MAX(CASE 
                        WHEN t1.sales_return_type_cd = 'fctr' THEN t1.total_transactions 
                        ELSE 0 
                    END) AS total_transactions_fctr,
                SUM(CAST(t1.total_sales_amt as NUMERIC)) AS total_sales_amt,
                MAX(CASE 
                        WHEN t1.sales_return_type_cd = 'fctr' THEN CAST(t1.total_sales_amt AS NUMERIC) 
                        ELSE 0 
                    END) AS total_sales_amt_fctr,
                SUM(CAST(t1.total_units_cnt AS NUMERIC)) AS total_units_cnt,
                MAX(CASE 
                        WHEN t1.sales_return_type_cd = 'fctr' THEN CAST(t1.total_units_cnt AS NUMERIC) 
                        ELSE 0 
                    END) AS total_units_cnt_fctr
            FROM (
                    SELECT 
                            f.country_id, 
                            f.sales_return_type_cd,
                            CASE 
                                WHEN TRIM(f.customer_id) = '0' 
                                OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                ELSE TRIM(f.customer_id)  
                            END AS customer_id,
                            COUNT(DISTINCT  CONCAT(f.country_id,f.business_unit_id,
                                            CAST(f.sales_return_dt AS STRING),
                                            f.sales_return_type_cd,
                                            CAST(f.sales_return_document_num AS STRING),
                                            CAST(f.location_id AS STRING))) AS total_transactions,
                            SUM(f.st_extended_net_amt) AS total_sales_amt,
                            SUM(f.unit_cnt) AS total_units_cnt
                    FROM `{}` AS f
                    INNER JOIN `{}` AS it   ON f.country_id = it.country_id 
                                                                                                    AND f.business_unit_id = it.business_unit_id                         
                                                                                                    AND SAFE_CAST(f.item_id AS INT64) = SAFE_CAST(it.item_id AS INT64)
                    INNER JOIN `{}` h ON CASE 
                                WHEN TRIM(f.customer_id) = '0' 
                                OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                ELSE TRIM(f.customer_id)  
                            END = h.customer_id 
                    LEFT JOIN `{}` e    ON CASE 
                                WHEN TRIM(f.customer_id) = '0' 
                                OR TRIM(f.customer_id) IS NULL THEN TRIM(f.main_customer_id) 
                                ELSE TRIM(f.customer_id)  
                            END = e.customer_id 
                    WHERE f.country_id = '{}'
                    AND f.business_unit_id='ENT'
                    AND f.sales_return_dt >= '{}' 
                    AND f.sales_return_dt < '{}'
                    AND f.sales_return_type_cd IN ('fctr', 'blta')
                    AND f.st_extended_net_amt > 0 
                    AND f.unit_cnt > 0
                    AND e.customer_id IS NULL 
                    GROUP BY f.country_id, 
                                 f.sales_return_type_cd,
                                 customer_id
                ) as t1
            GROUP BY
                 t1.country_id, 
                 t1.customer_id
        """.format(table_name, gcp_OH_tables['sales_table'], gcp_OH_tables['items_table'], fctr_table, ve_table, country_id, date_from, date_to)
        if country_id != 'CO':
            query_string = query_string + """
                HAVING MAX(CASE 
                                WHEN t1.sales_return_type_cd = 'fctr' THEN t1.total_sales_amt 
                                ELSE 0 
                            END) > 0
                ;
            """
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: createTemporalPROCustomerData || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise     

def createTemporalPROCustomerSet(country_id, period, date_from, date_to, percentile, gcp_project, gcp_dataset, bqclient, bqstorageclient):
    """
    createTemporalPROCustomerFiltered Function:
    Creates a table of Customers to consider typeB in the training exercise.
    This will filter and drop the [percentile(Int)]% of customers with worse performance in Sales, Transactions and Units

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    percentile int: percentile to filter
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createTemporalPROCustomerSet({}, {}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, percentile, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_customers_fctrpro_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        fctr_table = "{}.{}.segmentation_customers_fctrdata_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}` AS
            SELECT 
                customer_id, 
                total_transactions,
                total_transactions_fctr, 
                total_sales_amt,
                total_sales_amt_fctr,
                total_units_cnt,
                total_units_cnt_fctr 
            FROM 
            (
                SELECT 
                    customer_id, 
                    total_transactions,
                    total_transactions_fctr,
                    NTILE(100) OVER(ORDER BY total_transactions_fctr/total_transactions) AS decil_total_transactions,
                    total_sales_amt, 
                    total_sales_amt_fctr,
                    NTILE(100) OVER(ORDER BY total_sales_amt_fctr/total_sales_amt) AS decil_total_sales_amt,
                    total_units_cnt, 
                    total_units_cnt_fctr, 
                    NTILE(100) OVER(ORDER BY total_units_cnt_fctr/total_units_cnt) AS decil_total_units_cnt
                FROM `{}`
                WHERE total_transactions <> 0 
                AND total_transactions IS NOT NULL
                AND total_sales_amt <> 0
                AND total_sales_amt IS NOT NULL
                AND total_units_cnt <> 0
                AND total_units_cnt IS NOT NULL
            ) AS t3
        """.format(table_name, fctr_table)
        
        if country_id != 'CO':
            query_string = query_string + """
            WHERE decil_total_transactions > {} 
            AND decil_total_sales_amt > {} 
            AND decil_total_units_cnt > {}
            ;
            """.format(percentile, percentile, percentile)

        logging.info("SQL: "+ query_string)
        bqclient.query(query_string).result()
                
        #Get N PRO Customers
        query_string = """ 
                            SELECT 'TOTAL_PRO' as lbl, count(distinct customer_id) as V FROM `{}`
                            UNION ALL                
                            SELECT 'TOTAL_FCTR' as lbl, count(distinct customer_id) as V FROM `{}`
                            ORDER BY lbl """.format(table_name, fctr_table)

        logging.info("SQL: "+query_string)
        data = bqclient.query(query_string).result().to_dataframe(bqstorage_client=bqstorageclient)
        
        tot_fctr = data.iloc[0][1]
        tot_pro = data.iloc[1][1]
        pro_size = tot_pro / tot_fctr
        logging.info("FCTR CUSTOMERS: {}".format(tot_fctr))
        logging.info("PRO CUSTOMERS: {}".format(tot_pro))
        logging.info("PROPORTION AFTER FILTER: {0:.2f} %".format(pro_size*100))
        
        # End log
        logging.info("Finishing Function: createTemporalPROCustomerSet || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

def createTemporaltypeACustomerSet(country_id, period, date_from, date_to, n_times, gcp_project, gcp_dataset, bqclient, bqstorageclient):
    """
    createTemporaltypeACustomerSet Function:
    Creates a table with the Customers to consider as typeA Customers in the 
    training exercise. 

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    n_times int: how many times the size of the typeB Customer should it use
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createTemporaltypeACustomerSet({}, {}, {}, {}, {}, {}, {})".format(country_id, period, date_from, date_to, n_times, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.segmentation_customers_bltahog_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        pro_table = "{}.{}.segmentation_customers_fctrpro_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        blta_table = "{}.{}.segmentation_customers_blta_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #Get N typeB Customers
        query_string = "SELECT count(distinct customer_id) as V FROM `{}`".format(pro_table)
        logging.info("SQL: "+query_string)
        data = bqclient.query(query_string).result().to_dataframe(bqstorage_client=bqstorageclient)

        pro_size = data.iloc[0][0]
        logging.info("typeB CUSTOMERS: {}".format(pro_size))
        typeA_size = pro_size * n_times
        logging.info("typeA CUSTOMERS: {}".format(typeA_size))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        #Create Table
        query_string = """ 
                            CREATE TABLE `{}` AS
                            SELECT customer_id
                            FROM(
                              SELECT customer_id, ROW_NUMBER() OVER(ORDER BY RAND() DESC) AS RN
                              FROM `{}`
                            ) A
                            WHERE RN <= {} 
                            ;
                            """.format(table_name, blta_table, typeA_size)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End 
        logging.info("Finishing Function: createTemporaltypeACustomerSet || Time: {} sec.".format(str(round(time.time()-start))))    
    
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

def createTrainingSet(country_id, period, gcp_project, gcp_dataset, bqclient, sample=1.0):
    """
    createTrainingSet Function:
    Creates the Table with the Dataset for the algorithm
 to train.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    sample float: sets the proportion of total customers to use in the excersice.
        Default Value = 1.0 (100%)
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createTrainingSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        ###########################################################################
        #                             Set Table Names                             #
        ###########################################################################
        table_name = "{}.{}.segmentation_customers_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        pro_table  = "{}.{}.segmentation_customers_fctrpro_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        typeA_table = "{}.{}.segmentation_customers_bltahog_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table = "{}.{}.segmentation_features_set_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_dict_table   = "{}.{}.segmentation_features_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        #temp:
        table_name_temp = "{}.{}.segmentation_customers_tmp_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        table_name_temp2 = "{}.{}.segmentation_customers_tmp2_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        pro_temp = "{}.{}.segmentation_tmp_pro_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        typeA_temp = "{}.{}.segmentation_tmp_typeA_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        ###########################################################################
        #                           TMP typeB Customers                             #
        ###########################################################################
        query_drop = "DROP TABLE IF EXISTS `{}`".format(pro_temp)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        tmp_query = """ CREATE TABLE `{}` AS
                        SELECT  customer_id, 
                        ROW_NUMBER() OVER(ORDER BY RAND()) / COUNT(CUSTOMER_ID) OVER() AS perc
                        FROM `{}`
        """.format(pro_temp, pro_table)
        logging.info("SQL: "+tmp_query)
        bqclient.query(tmp_query).result()
                
        ###########################################################################
        #                           TMP typeA Customers                            #
        ###########################################################################
        query_drop = "DROP TABLE IF EXISTS `{}`".format(typeA_temp)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        tmp_query = """ CREATE TABLE `{}` AS
                        SELECT  customer_id, 
                        ROW_NUMBER() OVER(ORDER BY RAND()) / COUNT(CUSTOMER_ID) OVER() AS perc
                        FROM `{}`
        """.format(typeA_temp, typeA_table)
        logging.info("SQL: "+tmp_query) 
        bqclient.query(tmp_query).result()
        
        ###########################################################################
        #                               TMP STEP 1                                #
        ###########################################################################
        #Customers to use:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        logging.info("SQL: "+query_drop)    
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT customer_id, target
            FROM (
                SELECT  prof.customer_id,
                        1 as target
                FROM `{1:s}` prof
                WHERE prof.perc <= {3:f}
                UNION ALL
                SELECT  typeA.customer_id,
                        0 as target
                FROM `{2:s}` typeA
                WHERE typeA.perc <= {3:f}
            ) Z
            ;
        """.format(table_name_temp, pro_temp, typeA_temp, sample)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()

        ###########################################################################
        #                               TMP STEP 2                                #
        ###########################################################################
        #Features to use:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp2)
        logging.info("SQL: "+query_drop)    
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT A.*, B.feature_value, B.feature_name
            FROM `{1:s}` A 
            INNER JOIN `{2:s}` B ON A.customer_id = B.customer_id
            WHERE B.feature_name is not NULL
            ;
        """.format(table_name_temp2, table_name_temp, features_table)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()        
        
        ###########################################################################
        #                               TMP STEP 3                                #
        ###########################################################################
        #Dictionary:
        features_dict_table = "{}.{}.segmentation_features_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        customer_dict_table = "{}.{}.segmentation_customer_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(features_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT feature_name, (ROW_NUMBER() OVER())-1 as feature_idx
            FROM (
                SELECT DISTINCT A.feature_name
                FROM `{}` A
                ORDER BY A.feature_name
            )
        """.format(features_dict_table, table_name_temp2)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT customer_id, (ROW_NUMBER() OVER())-1 as customer_idx
            FROM (
                SELECT DISTINCT A.customer_id
                FROM `{}` A
            )
        """.format(customer_dict_table, table_name_temp2)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        ###########################################################################
        #                            Final Training Set                           #
        ###########################################################################
        #Dictionary:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)    
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT A.*, C.feature_idx, B.customer_idx
            FROM `{1:s}` A 
            INNER JOIN `{2:s}` B ON A.customer_id = B.customer_id
            INNER JOIN `{3:s}` C ON A.feature_name = C.feature_name
            ;
        """.format(table_name, table_name_temp2, customer_dict_table, features_dict_table)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        ###########################################################################
        #                            Drop TEMP Tables                             #
        ###########################################################################
        #drop TMP typeA table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(typeA_temp)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        #drop TMP PRO table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(pro_temp)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp2)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
                
        # End 
        logging.info("Finishing Function: createTrainingSet || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 
    
def createScoringSet(country_id, period, gcp_project, gcp_dataset, bqclient, customers_set_table):
    """
    createScoringSet Function:
    Creates a Table with the Dataset for the algorithm
 	to score.

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
        logging.info("Start Function: createScoringSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names:
        table_name = "{}.{}.segmentation_customers_scoringset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table = "{}.{}.segmentation_features_set_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_dict_table   = "{}.{}.segmentation_features_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        customers_dict_table = "{}.{}.segmentation_customers_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        model_table = "{}.{}.segmentation_predict_model_coef_{}".format(gcp_project,gcp_dataset,country_id)

        #drop table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customers_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string =  """
            CREATE TABLE `{0:s}` AS
            SELECT customer_id, target, (ROW_NUMBER() OVER()-1) as customer_idx
            FROM (
                    SELECT DISTINCT A.customer_id, A.target
                    FROM `{1:s}` A
                    INNER JOIN `{2:s}` B ON A.customer_id = B.customer_id
                    INNER JOIN `{3:s}` C ON B.feature_name = C.feature_name
                    WHERE B.feature_name IS NOT NULL
                    AND B.feature_value IS NOT NULL
                )
        """.format(customers_dict_table, customers_set_table, features_table, features_dict_table)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        #drop table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        #create Table:
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT A.customer_id, A.customer_idx, B.feature_name, C.feature_idx, B.feature_value, A.target
            FROM `{4:s}` A
            INNER JOIN `{2:s}` B ON A.customer_id = B.customer_id
            INNER JOIN `{3:s}` C ON B.feature_name = C.feature_name
            INNER JOIN `{5:s}` D ON B.feature_name = D.feature_name
            ;
        """.format(table_name, customers_set_table, features_table, features_dict_table, customers_dict_table, model_table)
        logging.info("SQL: " + query_string)
        bqclient.query(query_string).result()
                
        #drop table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customers_dict_table)
        logging.info("SQL: " + query_drop)
        bqclient.query(query_drop).result()
                
        # End 
        logging.info("Finishing Function: createScoringSet || Time: {} sec.".format(str(round(time.time()-start))))   
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

###############################################################################
#                     typeB by Necessity: Level 1                     #
#                  	 		(typeB_A - typeB_B)                   		  #
###############################################################################
def createTemporalAllPROL1CustomerSet(country_id, period, gcp_project, gcp_dataset, bqclient):
    """
    createTemporalAllPROL1CustomerSet Function:
    Create table with only PRO Customers to calculate features.

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
        logging.info("Start Function: createTemporalAllPROL1CustomerSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.pronecessity_lvl1_all_customers_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        pro_results_table = "{}.{}.customer_segmentation_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #pro_results_table = "{}.{}.segmentation_profile_results_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        query_string = """ CREATE TABLE `{}` AS
            SELECT customer_id 
            FROM `{}`
            WHERE company_profile = 'typeB'
        """.format(table_name, pro_results_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: createTemporalAllPROL1CustomerSet || Time: {} sec.".format(str(round(time.time()-start))))    
        return table_name
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 
        
def createTemporalAllPROL1CustomerNoOutSet(country_id, period, gcp_project, gcp_dataset, bqclient, outliers_rules):
    """
    createTemporalAllPROL1CustomerNoOutSet Function:
    Create table with only PRO Customers to calculate features.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    outliers_rules Dict: Dictionary with values needed for the Outliers treatment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createTemporalAllPROL1CustomerNoOutSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.pronecessity_lvl1_training_customers_noout_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        customers_set_table = "{}.{}.pronecessity_lvl1_all_customers_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))

                
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        query_create = """
            CREATE TABLE `{0:s}` AS
            SELECT  DISTINCT cs.customer_id
            FROM `{1:s}` ft
            INNER JOIN `{2:s}` cs ON ft.customer_id = cs.customer_id
            WHERE (feature_name = 'total_transactions' AND feature_value >= {3:s})
            OR (feature_name = '{4:s}' AND feature_value < (   SELECT APPROX_QUANTILES(COALESCE(feature_value, 0), 100)[OFFSET(75)] + ({5:s} * (APPROX_QUANTILES(COALESCE(feature_value, 0), 100)[OFFSET(75)]-APPROX_QUANTILES(COALESCE(feature_value, 0), 100)[OFFSET(25)]))
                                                                FROM `{1:s}` ftx 
                                                                RIGHT JOIN `{2:s}` csx ON ftx.customer_id = csx.customer_id
                                                                WHERE feature_name = 'total_sales_amt' ))
            GROUP BY cs.customer_id 
            HAVING count(distinct feature_name) = 2
        """.format( 
                    table_name, 
                    features_table, 
                    customers_set_table, 
                    str(outliers_rules['min_trx']),
                    outliers_rules['outlier_feature'], 
                    str(outliers_rules['sales_std_times'])
                )

        logging.info("SQL: "+query_create)
        bqclient.query(query_create).result()
                
        # End log
        logging.info("Finishing Function: createTemporalAllPROL1CustomerNoOutSet || Time: {} sec.".format(str(round(time.time()-start))))    
        return table_name
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

def createPRONecL1TrainingSet(country_id, period, gcp_project, gcp_dataset, bqclient, drop_outliers):
    """
    createPRONecL1TrainingSet Function:
    Creates the Table with the Dataset for the Necessity Level 1 (typeB_A) algorithm to train. Only customers with the 'PROFESIONAL' mark.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud Plataform BigQuery environment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createPRONecL1TrainingSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.pronecessity_lvl1_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        table_name_temp = "{}.{}.pronecessity_lvl1_tmp_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        logreg_result_table = "{}.{}.pronecessity_lvl1_all_customers_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))
        if drop_outliers:
            logreg_result_table = "{}.{}.pronecessity_lvl1_training_customers_noout_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))
        
        features_table = "{}.{}.pronecessity_lvl1_features_set_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        bqclient.query(query_drop).result()
        logging.info("SQL: "+query_drop)
        
        #Create Table with variables: per_trx_hier_department_cd
        query_string = """
            CREATE TABLE `{}` AS
            SELECT A.customer_id, B.feature_name, B.feature_value
            FROM `{}` AS A
            INNER JOIN `{}` AS B ON A.customer_id = B.customer_id
            ;
        """.format(table_name_temp, logreg_result_table, features_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)

        ###########################################################################
        #                            TMP Dictionaries                             #
        ###########################################################################
        #Dictionaries:
        features_dict_table = "{}.{}.pronecessity_lvl1_features_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        customer_dict_table = "{}.{}.pronecessity_lvl1_customer_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(features_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT feature_name, (ROW_NUMBER() OVER())-1 as feature_idx
            FROM (
                SELECT DISTINCT A.feature_name
                FROM `{}` A
                ORDER BY A.feature_name
            )
        """.format(features_dict_table, table_name_temp)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT customer_id, (ROW_NUMBER() OVER())-1 as customer_idx
            FROM (
                SELECT DISTINCT A.customer_id
                FROM `{}` A
            )
        """.format(customer_dict_table, table_name_temp)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        ###########################################################################
        #                            Final Training Set                            #
        ###########################################################################
        #Dictionary:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)    
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT A.*, C.feature_idx, B.customer_idx
            FROM `{1:s}` A 
            INNER JOIN `{2:s}` B ON A.customer_id = B.customer_id
            INNER JOIN `{3:s}` C ON A.feature_name = C.feature_name
            ;
        """.format(table_name, table_name_temp, customer_dict_table, features_dict_table)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()

        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        # End 
        logging.info("Finishing Function: createPRONecL1TrainingSet || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
    
def createPRONecL1ScoringSet(country_id, period, gcp_project, gcp_dataset, bqclient):
    """
    createtypeB_AScoringSet Function:
    Creates a Table with the Dataset for the algorithm to score.

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
        logging.info("Start Function: createPRONecL1ScoringSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.pronecessity_lvl1_scoringset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        table_name_temp = "{}.{}.pronecessity_lvl1_tmp_scoringset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        logreg_result_table = "{}.{}.pronecessity_lvl1_all_customers_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))
        features_table = "{}.{}.pronecessity_lvl1_features_set_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_dict_table = "{}.{}.pronecessity_lvl1_features_dictionary_{}".format(gcp_project, gcp_dataset, country_id)        
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        bqclient.query(query_drop).result()
        logging.info("SQL: "+query_drop)
        
        #Create Table with variables: per_trx_hier_department_cd
        query_string = """
            CREATE TABLE `{}` AS
            SELECT A.customer_id, B.feature_name, B.feature_value, C.feature_idx
            FROM `{}` AS A
            INNER JOIN `{}` AS B ON A.customer_id = B.customer_id
            INNER JOIN `{}` AS C ON B.feature_name = C.feature_name
            ;
        """.format(table_name_temp, logreg_result_table, features_table, features_dict_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)

        ###########################################################################
        #                            TMP Dictionaries                             #
        ###########################################################################
        #Dictionaries:
        customer_dict_table = "{}.{}.pronecessity_lvl1_customer_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
       
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT customer_id, (ROW_NUMBER() OVER())-1 as customer_idx
            FROM (
                SELECT DISTINCT A.customer_id
                FROM `{}` A
            )
        """.format(customer_dict_table, table_name_temp)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        ###########################################################################
        #                            Final Training Set                            #
        ###########################################################################
        #Dictionary:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)    
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT A.*, B.customer_idx
            FROM `{1:s}` A 
            INNER JOIN `{2:s}` B ON A.customer_id = B.customer_id
            ;
        """.format(table_name, table_name_temp, customer_dict_table, features_dict_table)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()

        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        # End 
        logging.info("Finishing Function: createPRONecL1ScoringSet || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

###############################################################################
#                     typeB by Necessity: Level 2                             #
#                          (L2A, L2B, L2C)                                    #
###############################################################################
def createTemporalAllPROL2CustomerSet(country_id, period, gcp_project, gcp_dataset, bqclient):
    """
    createTemporalAllPROL2CustomerSet Function:
    Create table with only PRO Customers to calculate features.

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
        logging.info("Start Function: createTemporalAllPROL2CustomerSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.pronecessity_lvl2_all_customers_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        pro_results_table = "{}.{}.segmentation_profile_results_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
                
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        query_string = """ CREATE TABLE `{}` AS
            SELECT customer_id 
            FROM `{}`
            WHERE company_profile = 'PROFESIONAL'
        """.format(table_name, pro_results_table)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
                
        # End log
        logging.info("Finishing Function: createTemporalAllPROL2CustomerSet || Time: {} sec.".format(str(round(time.time()-start))))    
        return table_name
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

def createPRONecL2TrainingSet(country_id, period, gcp_project, gcp_dataset, bqclient, drop_outliers):
    """
    createPRONecL2TrainingSet Function:
    Creates the Table with the Dataset for the Necessity Level 1 (typeB_A) algorithm to train. Only customers with the 'PROFESIONAL' mark.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud Plataform BigQuery environment
    drop_outliers bool: indicates if training would be done dropping outliers
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createPRONecL2TrainingSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.pronecessity_lvl2_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        table_name_temp = "{}.{}.pronecessity_lvl2_tmp_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        prev_result_table = "{}.{}.pronecessity_lvl2_all_customers_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))
        if drop_outliers:
            prev_result_table = "{}.{}.pronecessity_lvl2_training_customers_noout_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))
        features_table = "{}.{}.pronecessity_lvl2_features_set_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        bqclient.query(query_drop).result()
        logging.info("SQL: "+query_drop)
        
        #Create Table with variables: per_trx_hier_department_cd
        query_string = """
            CREATE TABLE `{}` AS
            SELECT A.customer_id, B.feature_name, B.feature_value
            FROM `{}` AS A
            INNER JOIN `{}` AS B ON A.customer_id = B.customer_id
            ;
        """.format(table_name_temp, prev_result_table, features_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)

        ###########################################################################
        #                            TMP Dictionaries                             #
        ###########################################################################
        #Dictionaries:
        features_dict_table = "{}.{}.pronecessity_lvl2_features_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        customer_dict_table = "{}.{}.pronecessity_lvl2_customer_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(features_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT feature_name, (ROW_NUMBER() OVER())-1 as feature_idx
            FROM (
                SELECT DISTINCT A.feature_name
                FROM `{}` A
            )
        """.format(features_dict_table, table_name_temp)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT customer_id, (ROW_NUMBER() OVER())-1 as customer_idx
            FROM (
                SELECT DISTINCT A.customer_id
                FROM `{}` A
            )
        """.format(customer_dict_table, table_name_temp)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        ###########################################################################
        #                            Final Training Set                           #
        ###########################################################################
        #Dictionary:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)    
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT A.*, C.feature_idx, B.customer_idx
            FROM `{1:s}` A 
            INNER JOIN `{2:s}` B ON A.customer_id = B.customer_id
            INNER JOIN `{3:s}` C ON A.feature_name = C.feature_name
            ;
        """.format(table_name, table_name_temp, customer_dict_table, features_dict_table)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()

        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        # End 
        logging.info("Finishing Function: createPRONecL2TrainingSet || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
    
def createPRONecL2ScoringSet(country_id, period, gcp_project, gcp_dataset, bqclient):
    """
    createPRONecL2ScoringSet Function:
    Creates a Table with the Dataset for the algorithm to score.

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
        logging.info("Start Function: createPRONecL2ScoringSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.pronecessity_lvl2_scoringset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        table_name_temp = "{}.{}.pronecessity_lvl2_tmp_scoringset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        prev_result_table = "{}.{}.pronecessity_lvl2_all_customers_{}{}".format(gcp_project,gcp_dataset, country_id, str(period))
        features_table = "{}.{}.pronecessity_lvl2_features_set_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_dict_table = "{}.{}.pronecessity_lvl2_features_dictionary_{}".format(gcp_project, gcp_dataset, country_id)        
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        bqclient.query(query_drop).result()
        logging.info("SQL: "+query_drop)
        
        #Create Table with variables: per_trx_hier_department_cd
        query_string = """
            CREATE TABLE `{}` AS
            SELECT A.customer_id, B.feature_name, B.feature_value, C.feature_idx
            FROM `{}` AS A
            INNER JOIN `{}` AS B ON A.customer_id = B.customer_id
            INNER JOIN `{}` AS C ON B.feature_name = C.feature_name
            ;
        """.format(table_name_temp, prev_result_table, features_table, features_dict_table)

        bqclient.query(query_string).result()
        logging.info("SQL: "+query_string)

        ###########################################################################
        #                            TMP Dictionaries                             #
        ###########################################################################
        #Dictionaries:
        customer_dict_table = "{}.{}.pronecessity_lvl2_customer_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
       
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT customer_id, (ROW_NUMBER() OVER())-1 as customer_idx
            FROM (
                SELECT DISTINCT A.customer_id
                FROM `{}` A
            )
        """.format(customer_dict_table, table_name_temp)

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        ###########################################################################
        #                            Final Training Set                            #
        ###########################################################################
        #Dictionary:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)    
        bqclient.query(query_drop).result()
                
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT A.*, B.customer_idx
            FROM `{1:s}` A 
            INNER JOIN `{2:s}` B ON A.customer_id = B.customer_id
            ;
        """.format(table_name, table_name_temp, customer_dict_table, features_dict_table)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()

        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name_temp)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        #drop TMP table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(customer_dict_table)
        logging.info("SQL: "+query_drop) 
        bqclient.query(query_drop).result()
        
        # End 
        logging.info("Finishing Function: createPRONecL1ScoringSet || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def createTemporalAllPROL2CustomerNoOutSet(country_id, period, gcp_project, gcp_dataset, bqclient, outliers_rules):
    """
    createTemporalAllPROL2CustomerNoOutSet Function:
    Create table with only PRO Customers to calculate features.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    outliers_rules Dict: Dictionary with values needed for the Outliers treatment
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: createTemporalAllPROL2CustomerNoOutSet({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.pronecessity_lvl2_training_customers_noout_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        customers_set_table = "{}.{}.pronecessity_lvl2_all_customers_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        features_table = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))

                
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
        
        query_create = """
            CREATE TABLE `{0:s}` AS
            SELECT  DISTINCT cs.customer_id
            FROM `{1:s}` ft
            INNER JOIN `{2:s}` cs ON ft.customer_id = cs.customer_id
            WHERE (feature_name = 'total_transactions' AND feature_value >= {3:s})
            OR (feature_name = '{4:s}' AND feature_value < (   SELECT APPROX_QUANTILES(COALESCE(feature_value, 0), 100)[OFFSET(75)] + ({5:s} * (APPROX_QUANTILES(COALESCE(feature_value, 0), 100)[OFFSET(75)]-APPROX_QUANTILES(COALESCE(feature_value, 0), 100)[OFFSET(25)]))
                                                                FROM `{1:s}` ftx 
                                                                RIGHT JOIN `{2:s}` csx ON ftx.customer_id = csx.customer_id
                                                                WHERE feature_name = 'total_sales_amt' ))
            GROUP BY cs.customer_id 
            HAVING count(distinct feature_name) = 2
        """.format( 
                    table_name, 
                    features_table, 
                    customers_set_table, 
                    str(outliers_rules['min_trx']),
                    outliers_rules['outlier_feature'], 
                    str(outliers_rules['sales_std_times'])
                )

        logging.info("SQL: "+query_create)
        bqclient.query(query_create).result()
                
        # End log
        logging.info("Finishing Function: createTemporalAllPROL2CustomerNoOutSet || Time: {} sec.".format(str(round(time.time()-start))))    
        return table_name
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise 

###############################################################################
###############################################################################
def createFinalResultsTable(country_id, period, gcp_project, gcp_dataset, bqclient):
    """ 
    createFinalResultsTable Function:
    Creates a table with the list of Customer and their respective profiles and segments.

    gcp_OH_tables Dict: Dictionary with the OH table names
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
        logging.info("Start Function: createFinalResultsTable({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        #prepare table names
        table_name = "{}.{}.customer_segmentation_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #profile table:
        profile_table = "{}.{}.segmentation_profile_results_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        #necessity_pro table:
        pronec_table = "{}.{}.pronecessity_lvl2_profile_results_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        #drop table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(table_name)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()

        query_string = """
            CREATE TABLE `{}` AS
            SELECT  p.country_id
                ,   p.customer_id
                ,   p.company_profile
                ,   n.necessity_lvl_2 as company_necessity
                ,   n.necessity_lvl_2_recency as company_recency_status
            FROM `{}` p
            LEFT JOIN `{}` n ON p.customer_id = n.customer_id
            ;
        """.format( 
                    table_name, 
                    profile_table, 
                    pronec_table
                    )

        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        # End log
        logging.info("Finishing Function: createFinalResultsTable || Time: {} sec.".format(str(round(time.time()-start)))) 
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise


