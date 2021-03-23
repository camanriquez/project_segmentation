#Profiling
""" 
Customer Classification: segmentmodels
@autor: Cristopher Manriquez
@description: Functions involving the models used in the Classification exercise.
Includes Training methods to create and calibrate new model's instances and the 
scoring methods for the already calibrated models

"""
###############################################################################
#                           Libraries and Imports                             #
###############################################################################
import logging
import numpy as np
import pandas as pd
import pickle
import sklearn.metrics as metrics
import time
import io
import os
import psutil
import xgboost as xgb
import segmentutil as ssu

from datetime import datetime
from scipy.sparse import csr_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.metrics import matthews_corrcoef
from google.cloud import bigquery


###############################################################################
#                       Training Functions Definitions                        #
###############################################################################
def trainNewModel(country_id, period, train_prop, test_prop, gcp_project, gcp_dataset, bqclient, bqstorageclient, bucket_name, choosen_model='xgboost' ,threshold=0.5, lr_max_iter=20000):
    """
    trainNewmodel Function:
    Trains a new model of Logistic Regression to classify . 
    The result would be a pickle file and a Table of parameters.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    train_prop float: should be between 0.0 and 1.0 and represent the proportion
        of the dataset to include in the train split. 
    test_prop float: should be between 0.0 and 1.0 and represent the proportion
        of the dataset to include in the test split.
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    threshold float: proabability value to overcome to be consider a typeB 
        Customer. Default: 0.5
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: trainNewModel({}, {}, {}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset, threshold, lr_max_iter))    
        
        if choosen_model == 'xgboost':
            choosen_model = 'XGBClassifier'
        else:
            choosen_model = 'LogisticRegression'
        ###########################################################################
        #                         Retrieving Data from GCP                        #
        ###########################################################################   
        # Get Training data (customers, target and features)
        short_lighttable_name = "segmentation_customers_trainingset_{}{}_light".format(country_id,str(period))
        lighttable_name = "{}.{}.segmentation_customers_trainingset_{}{}_light".format(gcp_project,gcp_dataset,country_id,str(period))
        
        table_name = "{}.{}.segmentation_customers_trainingset_{}{}".format(gcp_project,gcp_dataset,country_id,str(period))
        
        # Creates light version of the Original Table:
        query_drop = "DROP TABLE IF EXISTS `{}`;".format(lighttable_name)
        logging.info("SQL: {} - ".format(query_drop))
        bqclient.query(query_drop).result() # Waits for statement to complete.
        
        create_light = """
                            CREATE TABLE `{0:s}` AS 
                            SELECT  customer_idx
                                    ,feature_idx
                                    ,feature_value
                                    ,target
                            FROM `{1:s}` ;
                        """.format(lighttable_name, table_name)

        logging.info("SQL: "+ create_light)
        bqclient.query(create_light).result()
        
        # Import light table:
        logging.info("trainNewModel: Retrieving Data from BQS- Started")
        data = ssu.load_table_to_dataframe(gcp_project, gcp_dataset, short_lighttable_name, bucket_name)
        logging.info("trainNewModel: Retrieving Data from BQS- Finished")
        
        # Change retrieved Data format
        # getting size of original dataframe:
        buf_obj = io.StringIO()
        data.info(memory_usage='deep', buf=buf_obj)
        data_info = buf_obj.getvalue()
        logging.info("trainNewModel: Data retirieved Original Size - {}".format(data_info))
        logging.info("trainNewModel: CPU used: {}%".format(str(psutil.cpu_percent())))
        logging.info("trainNewModel: RAM used: {}%".format(psutil.virtual_memory()[2]))
        data = data.fillna(0)
       
        ###########################################################################
        #                    Define Training and Testing Sets                     #
        ###########################################################################
        logging.info("trainNewModel: Transform Data to Sparse Matrix - Started")
        data_X = csr_matrix((data['feature_value'].values, (data['customer_idx'].values, data['feature_idx'].values)))
        logging.info("trainNewModel: Transform Data to Sparse Matrix - Finished")
        
        #y Data
        data_y = data.groupby(['customer_idx', 'target']).size().reset_index(name='Freq').drop(['Freq'], axis = 1)
        data_y = data_y.set_index('customer_idx')
        np.random.seed(27218)
        #Train and Test Split
        logging.info("trainNewModel: Splitting Data - Started")
        X_train, X_test, y_train, y_test = train_test_split(data_X, data_y, train_size=train_prop, test_size=test_prop, random_state=27218)
        logging.info("\t \t trainNewModel: Splitting Data - Finished")
        logging.info("\t \t X_train: {}".format(X_train.shape))
        logging.info("\t \t X_test: {}".format(X_test.shape))
        logging.info("\t \t y_train: {}".format(y_train.shape))
        logging.info("\t \t y_test: {}".format(y_test.shape))
        #release memory from the original DataFrame 
        del data
        ###########################################################################
        #                                   Fit                                   #
        ########################################################################### 
        #Define Model
        if choosen_model== 'XGBClassifier':
            classifier=xgb.XGBClassifier(objective='binary:logistic', max_depth=20, n_estimators=400, n_jobs=-1, random_state=27218, learning_rate=0.1)
        else:
            classifier = LogisticRegression(n_jobs=-1, max_iter=lr_max_iter, solver='sag', random_state=27218)
            
        #Train Model
        start_fit = time.time()
        logging.info("trainNewModel: Fitting Model - Started")
        classifier.fit(X_train, np.ravel(y_train))
        logging.info("trainNewModel: Fitting Model - Finished || Time: {} sec.".format(str(round(time.time()-start_fit))))

        ###########################################################################
        #                                Evaluation                               #
        ###########################################################################     
        #accuracy_score:
        model_score = metrics.accuracy_score(np.ravel(y_test), classifier.predict(X_test))
        if choosen_model == 'XGBClassifier':
            logging.info('trainNewModel: Score XGBClassifier - ' + str(round(model_score, 4)*100) +'%')
        else:
            logging.info('trainNewModel: Score Logistic Regression - ' + str(round(model_score,4)*100) +'%')
            logging.info(str(classifier.n_iter_.max())+' iterations to converge')
        
        # matthews_corrcoef:
        mcc = matthews_corrcoef(np.ravel(y_test), classifier.predict(X_test))
        logging.info('trainNewModel: Matthews correlation coefficient - ' + str(round(mcc,4)*100) +'%')
        
        # Confusion Matrix:
        logging.info("trainNewModel: Confusion Matrix - ")
        tn, fp, fn, tp = confusion_matrix(np.ravel(y_test), classifier.predict(X_test)).ravel()
        conf_matrx = {'true_negatives': tn, 'false_positives': fp, 
                      'false_negatives': fn, 'true_positives': tp}
        cf_m = ""
        for k in conf_matrx:
            k_percent = (conf_matrx[k]/sum(conf_matrx.values()))*100
            cf_m = "{0:s} \n \t{1:s}: {2:.0f} ({3:.2f}%)".format(cf_m, k, conf_matrx[k], k_percent)
        logging.info(cf_m)
        
        ###########################################################################
        #                             Save Pickle File                            #
        ########################################################################### 
        logging.info('trainNewModel: Saving Model to Pickle File - Started')
        #define pickle file name and path:
        pickle_directory = "../DATA_MODELS/{}".format(country_id)
        pickle_file = 'segmentation_classifier.pickle'
        pickle_path = "{}/{}".format(pickle_directory, pickle_file)
        #check if folder exists:
        if not os.path.exists(pickle_directory):
            os.makedirs(pickle_directory)
        #check if pickle file exists:
        if os.path.isfile(pickle_path):
            #if exists, rename it (putting last modifictaion date in front of it)
            old_date = datetime.fromtimestamp(os.path.getmtime(pickle_path))
            old_date = old_date.strftime("%Y%m%d")
            os.rename(pickle_path,"{}/{}_{}".format(pickle_directory, old_date, pickle_file))
        #if exists, rename it:
        pickle_out = open(pickle_path,"wb")
        pickle.dump(classifier, pickle_out)
        pickle_out.close()
        logging.info("trainNewModel: Model saved in File '{}'".format(pickle_path))
        logging.info('trainNewModel: Saving Model to Pickle File - Finished')
                
        ###########################################################################
        #                            Saving Model Coef                            #
        ###########################################################################
        logging.info('trainNewModel: Saving Coefficients - Started')
        # Prepare DataFrame to Insert in table
        feature_coef = pd.DataFrame(columns=['country_id','model','creation_dttm', 'feature_idx', 'coef'])
        if choosen_model == 'XGBClassifier':
            feature_coef['coef'] = classifier.feature_importances_
        else:
            feature_coef['coef'] = classifier.coef_[0]
        
        feature_coef['coef'] = feature_coef['coef'].astype('float')
        #feature_coef['b0'] = classifier.intercept_[0]
        #feature_coef['b0'] = feature_coef['b0'].astype('float')
        feature_coef['feature_idx'] = feature_coef.index
        feature_coef['feature_idx'] = feature_coef['feature_idx'].astype('int')

        #Identity data to save in BQ:
        feature_coef['country_id'] = country_id
        feature_coef['country_id'] = feature_coef['country_id'].astype('str')
        feature_coef['model'] = choosen_model
        feature_coef['creation_dttm'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        #Create TMP Table to save model's coef
        model_table = "{}.{}.segmentation_predict_model_coef_{}".format(gcp_project,gcp_dataset,country_id)
        model_tmp_table = "{}.{}.segmentation_tmp_predict_model_coef_{}{}".format(gcp_project,gcp_dataset,country_id,str(period))
        features_dict_table   = "{}.{}.segmentation_features_dictionary_{}".format(gcp_project, gcp_dataset, country_id)
        
        #Drop STEP 1 table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`;".format(model_tmp_table)
        logging.info("SQL: {} - ".format(query_drop))
        bqclient.query(query_drop).result() # Waits for statement to complete.
        
        #Create STEP 1 table:
        query_create = """ 
            CREATE TABLE `{}`
            (
                country_id STRING,
                model STRING,
                creation_dttm STRING,
                feature_idx INT64,
                coef FLOAT64
            );
        """.format(model_tmp_table)
        logging.info("SQL: {} - ".format(query_create))
        bqclient.query(query_create).result()
        
        #Load DataFrame to Table:
        logging.info('trainNewModel: Loading Data from Dataframe - Started')
        job = bqclient.load_table_from_dataframe(feature_coef, model_tmp_table)
        job.result()  # Waits for table load to complete.
        assert job.state == "DONE"
        logging.info('trainNewModel: Loading Data from Dataframe - Finished')
        
        #Drop final table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(model_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        #Create final table:
        query_string = """
            CREATE TABLE `{}`
            AS 
            SELECT A.country_id,
                    A.model,
                    CAST(A.creation_dttm as DATETIME) as creation_dttm,
                    A.feature_idx,
                    A.coef,
                    B.feature_name
            FROM `{}` A
            INNER JOIN `{}` B ON A.feature_idx = B.feature_idx
            ORDER BY A.feature_idx;
        """.format(model_table, model_tmp_table, features_dict_table)
        logging.info("SQL: "+query_string)
        bqclient.query(query_string).result()
        
        #Drop TMP Tables:
        query_drop = "DROP TABLE IF EXISTS `{}`;".format(model_tmp_table)
        logging.info("SQL: {} - ".format(query_drop))
        bqclient.query(query_drop).result() # Waits for statement to complete.
                    
        logging.info('trainNewModel: Saving Coefficients - Finished')
        
        ###########################################################################
        #                        Save Test Set Predictions                        #
        ###########################################################################
        #Prepare DataFrame with results and predictions:
        start_aux = time.time()
        logging.info("trainNewModel: Scoring Data - Started")
        result_proba = classifier.predict_proba(X_test)
        logging.info("trainNewModel: Scoring Data - Finished || Time: {} sec.".format(str(round(time.time()-start_aux))))
        
        #format prediction:
        prob = pd.DataFrame(result_proba, columns=["prob_typeA","prob_typeB"])
        prob = prob.drop(["prob_typeA"], axis=1)
        y_test = y_test.reset_index()
        y_test = pd.concat([y_test, prob], axis =1)
        y_test["prediction"] = [1 if (x > threshold) else 0 for x in y_test["prob_typeB"]]
        
        ###########################################################################
        #                          Saving Results to BQ                           #
        ###########################################################################
        logging.info('trainNewModel: Saving Testing Results - Started')
        
        # Prepare DataFrame to Insert in table:
        y_test['country_id'] = country_id
        y_test['country_id'] = y_test['country_id'].astype('str')
        y_test['model'] = choosen_model
        y_test['classification_dttm'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        y_test['period'] = period
        
        #Create Table to save model's coef
        result_table = "{}.{}.segmentation_training_test_results_{}{}".format(gcp_project,gcp_dataset, country_id, period)
      
        #drop final table if exists
        query_drop = "DROP TABLE IF EXISTS `{}`".format(result_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        #create table:
        query_create = """ 
            CREATE TABLE `{}`
            (
                customer_idx INT64,
                target INT64,
                prob_typeB FLOAT64,
                prediction INT64,
                country_id STRING, 
                model STRING, 
                classification_dttm STRING, 
                period INT64
            );
        """.format(result_table)
        logging.info("SQL: {} - ".format(query_create))
        bqclient.query(query_create).result()
        
        #load dataframe into table:
        logging.info('trainNewModel: Loading Data from Dataframe - Started')
        job = bqclient.load_table_from_dataframe(y_test, result_table)
        job.result()  # Waits for table load to complete.
        assert job.state == "DONE"
        logging.info('trainNewModel: Loading Data from Dataframe - Finished')
        logging.info('trainNewModel: Saving Results - Finished')
        # End 
        logging.info("Finishing Function: trainNewModel || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise


def newScoringPy(country_id, period, gcp_project, gcp_dataset, gcp_OH_tables, date_to, bqclient, bqstorageclient, bucket_name, choosen_model='xgboost', threshold=0.5):
    """
    newScoringPy Function:
    Uses the LogisticRegression's score function to get the probability of 
    each customer of being a typeB Customer.
    
    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Cliente object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    threshold float: proabability value to overcome to be consider a typeB 
        Customer. Default: 0.5
    """
    try:
        # Start log:
        start = time.time()
        logging.info("Start Function: newScoringPy({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    
        
        if choosen_model == 'xgboost':
            choosen_model = 'XGBClassifier'
        else:
            choosen_model = 'LogisticRegression'
        
        #define pickle file name:
        pickle_directory = "../DATA_MODELS/{}".format(country_id)
        pickle_file = 'segmentation_classifier.pickle'
        pickle_path = "{}/{}".format(pickle_directory, pickle_file)
        
        #import model from the last piclke file:
        pickle_file = open(pickle_path, 'rb')
        classifier = pickle.load(pickle_file)
        pickle_file.close()
        
        ###########################################################################
        #                       Retrieving Data from GCP-BQ                       #
        ###########################################################################
        #Prepare table name and references:
        table_name = "{}.{}.segmentation_customers_scoringset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        
        lighttable_name = "{}.{}.segmentation_customers_scoringset_{}{}_light".format(gcp_project, gcp_dataset, country_id, str(period))
        short_lighttable = "segmentation_customers_scoringset_{}{}_light".format(country_id,str(period))
        
        # Creates light version of the Original Table:
        query_drop = "DROP TABLE IF EXISTS `{}`;".format(lighttable_name)
        logging.info("SQL: {} - ".format(query_drop))
        bqclient.query(query_drop).result() # Waits for statement to complete.
        
        create_light = """
                            CREATE TABLE `{0:s}` AS 
                            SELECT  customer_idx
                                    ,feature_idx
                                    ,feature_value
                            FROM `{1:s}` ;
                        """.format(lighttable_name, table_name)
        logging.info("SQL: "+ create_light)
        bqclient.query(create_light).result()
        
        # Get table data from BQ:
        start_aux = time.time()
        logging.info("newScoringPy: Retrieving Data - Started")
        #data = bqclient.list_rows(table).to_dataframe()
        data = ssu.load_table_to_dataframe(gcp_project, gcp_dataset, short_lighttable, bucket_name)
        data = data.fillna(0)
        logging.info("newScoringPy: Retrieving Data - Finished || Time: {} sec.".format(str(round(time.time()-start_aux))))
        
        ###########################################################################
        #                         Reducing DataFrame Size                         #
        ###########################################################################
        # getting size of original dataframe:
        buf_obj = io.StringIO()
        data.info(memory_usage='deep', buf=buf_obj)
        data_info = buf_obj.getvalue()
        logging.info("newScoringPy: Data retirieved Original Size - {}".format(data_info))
        
        # changing feature_value to float data type to downsize the Dataframe:
        # data['feature_value'] = data['feature_value'].apply(pd.to_numeric,downcast='float')
        
        # checking size of the new DataFrame:
        # buf_obj = io.StringIO()
        # data.info(memory_usage='deep', buf=buf_obj)
        # data_info = buf_obj.getvalue()
        # logging.info("newScoringPy: Data retirieved New Size - {}".format(data_info))
        
        ###########################################################################
        #                          Format Data to Score                           #
        ###########################################################################
        start_aux = time.time()
        logging.info("newScoringPy: Pivoting retrieved Data - Started")
        data_X = csr_matrix((data['feature_value'].values, (data['customer_idx'].values, data['feature_idx'].values)))
        logging.info("newScoringPy: Pivoting retrieved Data - Finished || Time: {} sec.".format(str(round(time.time()-start_aux))))
        #release memory from the original DataFrame 
        del data
        
        ###########################################################################
        #                                SCORE DATA                               #
        ###########################################################################
        start_aux = time.time()
        logging.info("newScoringPy: Scoring Data - Started")
        result_proba = classifier.predict_proba(data_X)
        logging.info("newScoringPy: Scoring Data - Finished || Time: {} sec.".format(str(round(time.time()-start_aux))))
        
        #format prediction
        result = pd.DataFrame(result_proba, columns=["prob_typeA","prob_typeB"])
        result = result.drop(["prob_typeA"], axis=1)
        result["customer_idx"] = result.index 
        result = result.reset_index(drop=True)
        result["prediction"] = [1 if (x > threshold) else 0 for x in result["prob_typeB"]]
        
        #Add id values
        result['country_id'] = country_id
        result['country_id'] = result['country_id'].astype('str')
        
        result['model'] = choosen_model
        class_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result['classification_dttm'] = class_date
        result['period'] = period
        #rearrange columns to match create table:
        result = result[["country_id","model","classification_dttm","period","customer_idx","prob_typeB","prediction"]]
        
        ###########################################################################
        #                          Saving Model Results                           #
        ###########################################################################
        logging.info('newScoringPy: Saving Results - Started')

        #Define table names:
        result_table = "{}.{}.segmentation_profile_results_{}{}".format(gcp_project,gcp_dataset, country_id, period)
        result_table_tmp = "{}.{}.segmentation_tmp_profile_results_{}{}".format(gcp_project,gcp_dataset, country_id, period)
        scoring_table = "{}.{}.segmentation_customers_scoringset_{}{}".format(gcp_project,gcp_dataset, country_id, period)
        fctrhist_table = "{}.{}.segmentation_customers_fctrhist_{}{}".format(gcp_project,gcp_dataset, country_id, period)
        ve_hist_table = "{}.{}.segmentation_customers_vehist_{}{}".format(gcp_project, gcp_dataset, country_id, period)

        #drop table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(result_table_tmp)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()
                
        #create table with algorithm's results:
        query_create = """ 
            CREATE TABLE IF NOT EXISTS `{}`
            (
                country_id STRING,
                model STRING,
                classification_dttm STRING,
                period INT64,
                customer_idx INT64,
                prob_typeB FLOAT64,
                prediction INT64            
            );
        """.format(result_table_tmp)
        logging.info("SQL: {} - ".format(query_create))
        bqclient.query(query_create).result()

        #load dataframe into table:
        logging.info('newScoringPy: Loading Data from Dataframe - Started')
        job = bqclient.load_table_from_dataframe(result, result_table_tmp)
        job.result()  # Waits for table load to complete.
        assert job.state == "DONE"
        logging.info('newScoringPy: Loading Data from Dataframe - Finished')
        

        ###########################################################################
        #                              Business Rules                             #
        ########################################################################### 
        #The business Rules are definitions that overwrite the result of the models score process:
        #
        # Business Rule - Factura (BR - Factura): it says that every customer 
        # with at least one fctr document in history will be considered typeB
        # with or without transactions in the last 24 Months.
        #
        # Business Rule - LP (BR - LP): Any customer who belongs or has belonged to the Loyalty Program
        # in one point of History, will be considered typeB.
        # with or without transactions in the last 24 Months. Overwrite previous rules.
        #
        # Business Rule - VVEE (BR - VVEE): Any customer who belongs or has belonged to the 
        # VVEE sales channel in one point of History, will be considered typeB.
        # with or without transactions in the last 24 Months. Overwrite previous rules.
        #
        # Business Rule - SII (BR - SII): Any customer with records in the SII (Servicio de impuestos Internos) 
        # with at least one transaction in history will be considered a typeB customer. Overwrite previous rules.
        #
        # Business Rule - GGEE (BR - GGEE): Any customer with records in the SII (Servicio de impuestos Internos) 
        # with a sales tranche greater than or equal to the one defined by the user and with at least one transaction in history 
        # will be considered "GGEE". Overwrite previous rules.
        #
        # Business Rule - SII CONT (BR - SII L2A): Any customer with records in the SII (Servicio de impuestos Internos) 
        # with a sales tranche greater than or equal to the one defined by the user and with at least one transaction in history 
        # will be considered "L2A" in th next step, ergo needs to be mark as typeB. Overwrite previous rules.
        #############################################################################
        
        logging.info('newScoringPy: Applying Business Rule: Factura - Started')
        #drop table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(result_table)
        logging.info("SQL: "+query_drop)
        bqclient.query(query_drop).result()

        # Business Rule - Factura (BR - Factura): it says that every customer 
        # with at least one fctr document in history will be considered typeB
        # with or without transactions in the last 24 Months.
        query_string = """
            CREATE TABLE `{0:s}` AS
            SELECT  A.country_id ,
                    CASE WHEN B.target = 1 then CONCAT(A.model, ' (BR - Factura)') ELSE A.model END as model ,
                    CAST(A.classification_dttm AS DATETIME) as classification_dttm,
                    A.period ,
                    A.customer_idx ,
                    A.prob_typeB ,
                    A.prediction ,
                    B.customer_id,
                    B.target,
                    CASE WHEN B.target = 1 OR A.prediction = 1 THEN 'typeB' 
                         ELSE 'typeA'
                    END AS company_profile
            FROM `{1:s}` A
            INNER JOIN  (
                          SELECT DISTINCT customer_id, customer_idx, target
                          FROM `{2:s}`
                         ) B ON A.customer_idx = B.customer_idx
            ;
        """.format( result_table, 
                    result_table_tmp, 
                    scoring_table)
        
        logging.info("SQL: " + query_string)
        bqclient.query(query_string).result()
        
        #insert Business rules: Customers fctr (not 24 months)
        insert_string = """ 
                        INSERT INTO `{0:s}`
                        SELECT '{1:s}' as country_id
                                   ,'{6:s} (BR - Factura)' as model
                               ,CAST('{2:s}' AS DATETIME) as classification_dttm
                               ,{3:s} as period
                               ,NULL as customer_idx
                               ,NULL as prob_typeB
                               ,NULL as prediction
                               ,A.customer_id
                               ,1 as target
                               ,'typeB' as company_profile
                        FROM  `{4:s}` A
                        LEFT JOIN (SELECT DISTINCT customer_id FROM `{5:s}`) B ON A.customer_id = B.customer_id
                        WHERE B.customer_id IS NULL 
                        """.format(result_table, country_id, class_date, str(period), fctrhist_table, scoring_table, choosen_model)
        logging.info("SQL: " + insert_string)
        bqclient.query(insert_string).result()
       
        #drop tmp table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(result_table_tmp)
        logging.info("SQL: " + query_drop)
        bqclient.query(query_drop).result()
        logging.info('newScoringPy: Applying Business Rule: Factura - Finished')

        
        # Business Rule - LP (BR - LP): Any customer who belongs or has belonged to the Loyalty Program 
        # in one point of History, will be considered typeB.
        # with or without transactions in the last 24 Months. Overwrite previous rules.
        logging.info('newScoringPy: Applying Business Rule: LP - Started')
        # First insert customers LP with no transactions in the last 24 Months
        insert_string = """ 
                        INSERT INTO `{0:s}`
                        SELECT '{1:s}' as country_id
                               ,'{5:s} (BR - LP)' as model
                               ,CAST('{2:s}' AS DATETIME) as classification_dttm
                               ,{3:s} as period
                               ,NULL as customer_idx
                               ,NULL as prob_typeB
                               ,NULL as prediction
                               ,A.customer_id
                               ,NULL as target
                               ,'typeB'
                        FROM `{4:s}` A
                        LEFT JOIN `{0:s}` B  ON A.customer_id = B.customer_id
                        LEFT JOIN `{6:s}` BL ON A.customer_id = BL.customer_id AND A.country_id = BL.country_id
                        LEFT JOIN `{7:s}` VE ON A.customer_id = VE.customer_id
                        WHERE B.customer_id IS NULL
                        AND BL.customer_id IS NULL
                        AND VE.customer_id IS NULL
                        AND A.country_id = '{1:s}'
                        """.format( result_table, 
                                    country_id, 
                                    class_date, 
                                    str(period), 
                                    gcp_OH_tables['ces_table'],
                                    choosen_model, 
                                    gcp_OH_tables['black_list_table'],
                                    ve_hist_table)
        logging.info("SQL: " + insert_string)
        bqclient.query(insert_string).result()
        # Then, overwrite all previous rules updating all LP customers
        update_string = """ 
                            UPDATE `{0:s}`
                            SET model = '{3:s} (BR - LP)',
                                company_profile = 'typeB'
                            WHERE customer_id IN (SELECT customer_id FROM `{1:s}` WHERE country_id = '{2:s}')
                        """.format( result_table, 
                                    gcp_OH_tables['ces_table'],
                                    country_id,
                                    choosen_model)
        logging.info("SQL: " + update_string)
        bqclient.query(update_string).result()
        logging.info('newScoringPy: Applying Business Rule: LP - Finished')
        
        
        # Business Rule - VVEE (BR - VVEE): Any customer who belongs or has belonged to the 
        # VVEE sales channel in one point of History, will be considered typeB.
        # with or without transactions in the last 24 Months. Overwrite previous rules.
        logging.info('newScoringPy: Applying Business Rule: VVEE- Started')
        # Insert Customers VVEE Hist. Customers VVEE are excluded from  the entire process
        # so an Update sentence as in the previous Business rule is not necessary
        insert_string = """ 
                        INSERT INTO `{0:s}`
                        SELECT '{1:s}' as country_id
                               ,'{5:s} (BR - VVEE PRO)' as model
                               ,CAST('{2:s}' AS DATETIME) as classification_dttm
                               ,{3:s} as period
                               ,NULL as customer_idx
                               ,NULL as prob_typeB
                               ,NULL as prediction
                               ,A.customer_id
                               ,1 as target
                               ,'typeB'
                        FROM `{4:s}` A
                        LEFT JOIN `{0:s}` B ON A.customer_id = B.customer_id
                        WHERE B.customer_id IS NULL
                        """.format( result_table, 
                                    country_id, 
                                    class_date, 
                                    str(period), 
                                    ve_hist_table,
                                    choosen_model)
        logging.info("SQL: " + insert_string)
        bqclient.query(insert_string).result()
        logging.info('newScoringPy: Applying Business Rule: VVEE - Finished')
        
        # Business Rule - SII (BR - SII): Any customer with records in the SII (Servicio de impuestos Internos) 
        # with at least one transaction in history will be considered a typeB customer. Overwrite previous rules.      
        logging.info('newScoringPy: Applying Business Rule: SII- Started')
        ggee_table_name = "{}.{}.segmentation_customers_ggeehist_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))  
        
        insert_string = """ 
                        INSERT INTO `{0:s}`
                        SELECT  '{1:s}' as country_id
                                ,'{4:s} (BR - SII)' as model
                                ,CAST('{2:s}' AS DATETIME) as classification_dttm
                                ,{3:s} as period
                                ,NULL as customer_idx
                                ,NULL as prob_typeB
                                ,NULL as prediction
                                ,A.customer_id
                                ,NULL as target
                                ,'typeB' 
                        FROM `{5:s}` A 
                        LEFT JOIN `{0:s}` B ON A.customer_id = B.customer_id
                        WHERE sales_tranche < {6:s}
                        AND B.customer_id IS NULL
                        """.format( result_table, 
                                    country_id, 
                                    class_date, 
                                    str(period), 
                                    choosen_model,
                                    ggee_table_name, 
                                    str(gcp_OH_tables['ggee_sales_tranche'])
                                )
        logging.info("SQL: " + insert_string)
        bqclient.query(insert_string).result()
        
        # Then, overwrite all previous rules updating all SII customers
        update_string = """ 
                            UPDATE `{0:s}`
                            SET model = '{2:s} (BR - SII)',
                                company_profile = 'typeB'
                            WHERE customer_id IN (SELECT customer_id FROM `{1:s}`)
                        """.format( result_table, 
                                    ggee_table_name,
                                    choosen_model)
        logging.info("SQL: " + update_string)
        bqclient.query(update_string).result()
        
        logging.info('newScoringPy: ApplyingBusiness Rule: SII- Finished')
        
        # Business Rule - GGEE (BR - GGEE): Any customer with records in the SII (Servicio de impuestos Internos) 
        # with a sales tranche greater than or equal to the one defined by the user and with at least one transaction in history 
        # will be considered "GGEE". Overwrite previous rules.
        logging.info('newScoringPy: Applying Business Rule: GGEE- Started')
        # Insert Customers GGEE Hist. Customers GGEE are excluded from  the entire process
        # so an Update sentence as in the previous Business rule is not necessary
        insert_string = """ 
                        INSERT INTO `{0:s}`
                        SELECT  '{1:s}' as country_id
                                ,'{4:s} (BR - GGEE SII)' as model
                                ,CAST('{2:s}' AS DATETIME) as classification_dttm
                                ,{3:s} as period
                                ,NULL as customer_idx
                                ,NULL as prob_typeB
                                ,NULL as prediction
                                ,customer_id
                                ,NULL as target
                                ,'GGEE' 
                        FROM `{5:s}`
                        WHERE sales_tranche >= {6:s}
                        """.format( result_table, 
                                    country_id, 
                                    class_date, 
                                    str(period), 
                                    choosen_model,
                                    ggee_table_name, 
                                    str(gcp_OH_tables['ggee_sales_tranche'])
                                )
        logging.info("SQL: " + insert_string)
        bqclient.query(insert_string).result()
        logging.info('newScoringPy: Applying Business Rule: GGEE - Finished')
        
        # Business Rule - SII CONT (BR - L2A SII): Any customer with records in the SII (Servicio de impuestos Internos) 
        # with a sales tranche greater than or equal to the one defined by the user and with at least one transaction in history 
        # will be considered "L2A" in th next step, ergo needs to be mark as typeB. Overwrite previous rules.        
        # Inserts Customers SSII with no transactions in the past 24 Months:
        logging.info('newScoringPy: Applying Business Rule: L2A SII')
        insert_string = """ 
                        INSERT INTO `{0:s}`
                        SELECT  '{1:s}' as country_id
                                ,'{4:s} (BR - L2A SII)' as model
                                ,CAST('{2:s}' AS DATETIME) as classification_dttm
                                ,{3:s} as period
                                ,NULL as customer_idx
                                ,NULL as prob_typeB
                                ,NULL as prediction
                                ,A.customer_id
                                ,NULL as target
                                ,'typeB' 
                        FROM `{5:s}` A
                        LEFT JOIN `{0:s}` B ON A.customer_id = B.customer_id
                        WHERE B.customer_id IS NULL
                        AND sales_tranche >= {6:s}
                        AND sales_tranche < {7:s}
                        """.format( result_table, 
                                    country_id, 
                                    class_date, 
                                    str(period), 
                                    choosen_model,
                                    ggee_table_name, 
                                    str(gcp_OH_tables['cont_sales_tranche']),
                                    str(gcp_OH_tables['ggee_sales_tranche'])
                                )
        logging.info("SQL: " + insert_string)
        bqclient.query(insert_string).result()
        # Update Customers with a sales tranche between the ones defined by the user:
        update_string = """ 
                            UPDATE `{0:s}`
                            SET model = '{1:s} (BR - L2A SII)',
                                company_profile = 'typeB'
                            WHERE customer_id IN (  SELECT customer_id 
                                                    FROM `{2:s}` 
                                                    WHERE sales_tranche >= {3:s}
                                                    AND sales_tranche < {4:s} )
                        """.format( result_table, 
                                    choosen_model,
                                    ggee_table_name, 
                                    str(gcp_OH_tables['cont_sales_tranche']),
                                    str(gcp_OH_tables['ggee_sales_tranche']),
                                    choosen_model
                                )
        logging.info("SQL: " + update_string)
        bqclient.query(update_string).result()
        logging.info('newScoringPy: ApplyingBusiness Rule: L2A SII - Finished')
        
        
        logging.info('newScoringPy: Saving Results - Finished')        
        # End 
        logging.info("Finishing Function: newScoringPy || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise
