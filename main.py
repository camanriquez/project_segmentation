#main
"""  
Customer Classification: Main Process File
@autor: Cristopher Manriquez
@description: Pipeline for the Customer's classification and clusterization

"""
import numpy as np
import logging
import segmentutil as ssu
import segmentfeatures as ssf
import segmentcustomers as ssc
import segmentmodel_profile as sspr
import segmentmodel_necessity as ssnc
import time
import os
import sys
import inspect
from datetime import datetime

###############################################################################
#                            SET PARAMETERS (Start)                           #
###############################################################################
###############################################################################
#                        PARAMETERS: Global Parameters                        #
###############################################################################
country_id  = 'CL'                      # Country id 
period  =  202001                       # Month to process
months  =  24                           # Months of Data to use in the Process 
gcp_project = 'gcp-proyect' # GCP project
gcp_dataset = 'gcp_dataset'      # GCP dataset where the tables of the exercise will be written
gcp_bucket = 'segmentacion_seg_{}{}'.format(country_id.lower(), str(period))    #GCP Bucket in Storage, if it doesn't exists creates
# Dictionary with production enviorment tables to use:
#If target_table is set to None it will use all the customers with at least 1 document fctr 
gcp_OH_tables = {   
                    'sales_table':'main-data.sales_data.sales',
                    'items_table':'main-data.sales_data.item',

                    'black_list_table':'sandbox.segmentation.exclusions',
                    'target_table':'sandbox.segmentation.companies',
                    'items_pro_table': 'sandbox.segmentation.main_skus',
                    'ces_table':'sandbox.segmentation.main_customers',
                    'ggee':'sandbox.segmentation.sii_data',
                    
                    'ggee_sales_tranche':'7',  # GGEE's minimun Sales tranche to be consider as GGEE
                    'cont_sales_tranche':'4'   # GGEE's minimun Sales tranche to be consider as L2A
                }
###############################################################################
#                       PARAMETERS: typeA - TypeB                             #
###############################################################################
profile = True            # True to process the segmentation typeA - TypeB block, False to skip
training = True           # True if a new model will be train. False to only score previous model.
percentile = 5            # Percentile of 'fctr' customers to drop.
n_times = 4               # Proportion of Home customers to use in the training exercise.
sample =  1.0             # Size of the sample to create to train the new Model.
train_prop = 0.7          # Percentage of the sample to use as train set in the training exercise.
test_prop = 0.3           # Percentage of the sample to use as test set in the training exercise.
choosen_model = 'xgboost' # Default 'xgboost', optional:'logreg'
lr_max_iter = 20000       # Only for 'logreg' choosen_model. Max iter parameter for Logistic regression.
threshold = 0.5           # Threshold to  surpass to be consider positive in the scoring process.
#list of features to use:
feature_list = ['avg_units_trx','per_sales_ntcr','per_trx_channel_%',
                'per_trx_cuchi_store','per_trx_hour_%','per_trx_N0%',
                'per_trx_N1%','per_trx_N2%','per_trx_ntcr',
                'per_trx_store_%','per_trx_weekday_%','per_uni_N0%',
                'per_uni_N1%','per_uni_N2%','set_diversity',
                'skus_N1%','total_stores','trx_N1%','item_pro_%']

standardize = True  # True if values need to be standardized (Z will be used)
std_type = 'MinMax' # 'Z' or 'MinMax'

# List of features to std (Empty List means every feature, 
# Set pronec_l1_std to False to not standardize any feature):
std_list =['avg_units_trx','set_diversity','skus_N1%',
           'total_stores','trx_N1%', 'per_sales_ntcr','per_trx_ntcr'] 

###############################################################################
# PARAMETERS: Professional Necessity Level 2 (L2A, L2B, L2C) #
###############################################################################
pronec_l2= True             # True to process the segmentation Pro Level 2 block, False to skip  
pronec_l2_training = True    # True if a new model will be train. False to only score previous model.
pronec_l2_init = 'random'    # How will the kmeans algorithm will be initialized, other optionas are: 'k-means++' and 'PCA'
pronec_l2_iter = 1000        # Max iter parameter for the K-means algorithm

#list of features to use:
pronec_l2_feature_list = ['total_sales_amt', # for effects of plotting this feature is Mandatory
                          'per_trx_frec_N0', # for effects of plotting this feature is Mandatory
                          'months_since_last_trx']

pronec_l2_std = True       # True if values need to be standardized (Z will be used)
pronec_l2_std_type = 'MinMax'   # 'Z' or 'MinMax'
#list of features to std:
pronec_l2_std_list = ['total_sales_amt','months_since_last_trx']     # Empty List means every feature - Set pronec_l2_std to False to not standardize any feature

pronec_l2_drop_outliers = True #True if outliers need to be droped from training
pronec_l2_outliers_rules = {
                            'min_trx':3, #Minimun transactions for the customer to be consider in training
                            'outlier_feature':'total_sales_amt', #feature to use to drop outliers
                            'sales_std_times':3 # The drop process will drop outliers using the following rule: percentile_75(outlier_feature) + (X*IQR(outlier_feature))
                            }

###############################################################################
#                             SET PARAMETERS (End)                            #
###############################################################################

###############################################################################
#                              Start Processing                               #
###############################################################################   
#Set random seed:
np.random.seed(27218)

#create bucket needed
if ssu.check_bucket_exists(gcp_bucket) == False:
    ssu.create_temp_bucket(gcp_bucket)

#Secure current working directory:
def getScriptDir(follow_symlinks=True):
    """
    getScriptDir Function:
    returns the current directory where the Scripts is running. 
    This funciton must be in the main file.
    """
    if getattr(sys, 'frozen', False): # py2exe, PyInstaller, cx_Freeze
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(getScriptDir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path)
os.chdir(getScriptDir())

#Set log parameters:
logd = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(filename='{}{}_results_modules_{}.log'.format(country_id, str(period), logd), 
                    level=logging.INFO, 
                    format='%(asctime)s %(message)s', 
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filemode='w')

# Start log
start = time.time()
logging.info("Start Main Function")


###############################################################################
#                          Get Dates and Connections                          #
###############################################################################   
# Get dates for process
process_dates = ssu.getDates(period, months)
date_from = process_dates['date_from']
date_to = process_dates['date_to']

#connect to BigQuery
bqclient, bqstorageclient = ssu.getBQClient()

###############################################################################
#                  SEGMENTATION: typeA - TypeB (Start)                        #
###############################################################################
if profile == True:
    ###########################################################################
    #                         Prepare Customers sets                          #
    ########################################################################### 
    ssc.createTemporalVEECustomers(gcp_OH_tables,
                                   country_id, 
                                   period,
                                   date_to,
                                   gcp_project, 
                                   gcp_dataset, 
                                   bqclient)
    
    ssc.createTemporalGGEECustomers(gcp_OH_tables,
                                   country_id, 
                                   period,
                                   date_to,
                                   gcp_project, 
                                   gcp_dataset, 
                                   bqclient)
    
    ssc.createTemporalFCTRCustomers(gcp_OH_tables, 
                                    country_id, 
                                    period,
                                    date_to,
                                    gcp_project, 
                                    gcp_dataset, 
                                    bqclient)

    ssc.createTemporalBLTACustomers(gcp_OH_tables, 
                                    country_id, 
                                    period, 
                                    process_dates['date_from'], 
                                    process_dates['date_to'], 
                                    gcp_project, 
                                    gcp_dataset, 
                                    bqclient)

    customers_set_table = ssc.createTemporalAllCustomerSet(country_id, 
                                                            period, 
                                                            gcp_project, 
                                                            gcp_dataset,
                                                            bqclient)
    
    customers_set_table_ve = ssc.createTemporalAllCustomerSetVE(country_id, 
                                                            period, 
                                                            gcp_project, 
                                                            gcp_dataset,
                                                            bqclient)
    
    ###########################################################################
    #                         Prepare Feature Values                          #
    ########################################################################### 
    #Create empty table to store Features:    
    ssf.createFeaturesTable(country_id,
                            period, 
                            gcp_project, 
                            gcp_dataset, 
                            bqclient)

    #Insert All features for Classifier:
    ssf.insertallFeatures(gcp_OH_tables, 
                          country_id, 
                          period, 
                          process_dates, 
                          customers_set_table_ve, 
                          training, 
                          gcp_project, 
                          gcp_dataset, 
                          bqclient)
    ###########################################################################
    #                           Prepare Feature Set                           #
    ########################################################################### 
    ssf.createFeaturesSet(country_id,           #Country processing
                          period,               #Period to calculate
                          gcp_project,          #BQ project to use
                          gcp_dataset,          #BQ Dataset where tables will be created
                          bqclient,             #Client to connect with BQ
                          training,             #True if executing a Training exercise
                          customers_set_table,  #Universe of Customers to use (Training + Scoring)
                          feature_list,         #Features to include
                          standardize,          #True for Standardization Process (Z)
                          std_list,             #Features to standardize
                          std_type,             #Standardization Function
                          'segmentation')       #Segementation Level

    ###########################################################################
    #                             Train New Model                             #
    ###########################################################################  
    #Prepare Data Set to train Classification Model
    if training == True:
    # If is training prepare Create sets of typeB Customers, Home Customers and Training Customers
        ssc.createTemporalPROCustomerData(gcp_OH_tables, 
                                          country_id, 
                                          period, 
                                          date_from, 
                                          date_to, 
                                          gcp_project, 
                                          gcp_dataset, 
                                          bqclient)

        ssc.createTemporalPROCustomerSet(country_id, 
                                         period, 
                                         date_from, 
                                         date_to, 
                                         percentile,
                                         gcp_project, 
                                         gcp_dataset, 
                                         bqclient,
                                         bqstorageclient)

        ssc.createTemporalHOMECustomerSet(country_id,
                                          period, 
                                          date_from, 
                                          date_to, 
                                          n_times, 
                                          gcp_project, 
                                          gcp_dataset, 
                                          bqclient,
                                          bqstorageclient)

        ssc.createTrainingSet(country_id, 
                              period, 
                              gcp_project, 
                              gcp_dataset, 
                              bqclient, 
                              sample)
        #TRAIN MODEL
        bqclient, bqstorageclient = ssu.getBQClient()
        sspr.trainNewModel(country_id, 
                           period,
                           train_prop,
                           test_prop, 
                           gcp_project,
                           gcp_dataset, 
                           bqclient,
                           bqstorageclient,
                           gcp_bucket,
                           choosen_model,
                           threshold,
                           lr_max_iter
                           )

    ###########################################################################
    #                             Score New Model                             #
    ###########################################################################   
    #prepare Scoring set of customers:
    ssc.createScoringSet(country_id, 
                         period, 
                         gcp_project, 
                         gcp_dataset, 
                         bqclient, 
                         customers_set_table)
    #SCORE
    bqclient, bqstorageclient = ssu.getBQClient()
    sspr.newScoringPy(country_id, 
                      period, 
                      gcp_project, 
                      gcp_dataset, 
                      gcp_OH_tables, 
                      date_to,
                      bqclient, 
                      bqstorageclient,
                      gcp_bucket,
                      choosen_model,
                      threshold)
    
###############################################################################
#                   SEGMENTATION: typeA - TypeB  (End)                        #
###############################################################################

###############################################################################
#             SEGMENTATION: typeB Necessity Level 2 (Start)            #
###############################################################################
if pronec_l2 == True:
    bqclient, bqstorageclient = ssu.getBQClient()
    ###########################################################################
    #                            Prepare Customers                            #
    ###########################################################################
    customersl2_set_table = ssc.createTemporalAllPROL2CustomerSet(country_id, period, gcp_project, gcp_dataset, bqclient)    
    
    if pronec_l2_drop_outliers:
        ssc.createTemporalAllPROL2CustomerNoOutSet(country_id, period, gcp_project, gcp_dataset, bqclient, pronec_l2_outliers_rules)
        
    ###########################################################################
    #                             Prepare Features                            #
    ########################################################################### 
    ssf.createFeaturesSet(country_id,             # Country processing
                          period,                 # Period to calculate
                          gcp_project,            # BQ project to use
                          gcp_dataset,            # BQ Dataset where tables will be created
                          bqclient,               # Client to connect with BQ
                          pronec_l2_training,     # True if executing a Training exercise
                          customersl2_set_table,  # Universe of Customers to use (Training + Scoring)
                          pronec_l2_feature_list, # Features to include
                          pronec_l2_std,          # True for Standardization Process (Z)
                          pronec_l2_std_list,     # Features to standardize
                          pronec_l2_std_type,     # Standardization Function
                          'pronecessity_lvl2',    # Segementation Level
                          pronec_l2_drop_outliers)    
    ###########################################################################
    #                             Train New Model                             #
    ########################################################################### 
    if pronec_l2_training == True:
        #Create training Data Set:
        ssc.createPRONecL2TrainingSet(country_id, 
                                      period, 
                                      gcp_project, 
                                      gcp_dataset, 
                                      bqclient,
                                      pronec_l2_drop_outliers)
        
        ssnc.PRONecL2NewModel(country_id, 
                              period, 
                              gcp_project, 
                              gcp_dataset, 
                              bqclient, 
                              bqstorageclient, 
                              pronec_l2_init, 
                              pronec_l2_iter,
                              gcp_bucket)
    
    ###########################################################################
    #                             Score New Model                             #
    ###########################################################################
    #Create scoring Data Set:
    #ssc.createPRONecL2ScoringSet(country_id, period, gcp_project, gcp_dataset, bqclient)
    #SCORE MODEL:
    ssnc.PRONecL2NewScoring(country_id, period, gcp_project, gcp_dataset, bqclient, bqstorageclient, pronec_l2_outliers_rules)
###############################################################################
#              SEGMENTATION: Professional Necessity Level 2 (End)             #
###############################################################################

    ssc.createFinalResultsTable(country_id, period, gcp_project, gcp_dataset, bqclient)

#Drop temporal tables:
ssu.dropTemporalTables(country_id, period, date_to, gcp_project, gcp_dataset, bqclient)
#Delete Storage:
ssu.delete_complete_bucket(gcp_bucket)
#RENAME RESULTS FOLDER
os.rename('DATA_MODELS/{}/'.format(country_id), 'DATA_MODELS/{}{}/'.format(country_id, str(period)))

#End
logging.info("Main function Finished || Time: {} sec.".format(str(round(time.time()-start))))  
logging.shutdown()