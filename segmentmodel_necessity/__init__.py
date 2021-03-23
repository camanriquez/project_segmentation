#model
""" 
Customer Classification: segmentmodels_necessity
@autor: Cristopher Manriquez
@description: Functions involving the models used in the Classification exercise
Include Training methods to create and calibrate new model's instances and the 
scoring methods for the already calibrated models

"""
###############################################################################
#                           Libraries and Imports                             #
###############################################################################
import io
import os
import time
import psutil
import pickle
import logging
import graphviz
import numpy as np
import pandas as pd
import segmentutil as ssu


from sklearn import tree
from sklearn import metrics
from datetime import datetime
from google.cloud import bigquery
from sklearn.cluster import KMeans
from scipy.sparse import csr_matrix
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA

###############################################################################
#         typeB by Necessity: Level 1 (typeB_A - typeB_B)          #
###############################################################################
def PRONecL1NewModel(country_id, period, gcp_project, gcp_dataset, bqclient, bqstorageclient, pronec_l1_init, pronec_l1_iter, bucket_name):
    """
    PRONecL1NewModel Function:
    K-Means over the typeB customers base to create 2 clusters: typeB_A / typeB_B. 
    It's expected that typeB_A Customers have a lower transactions concentration in the Departments 1 and 2
    The result would be a pickle file.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Client object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    bqstorageclient obj: BigQuery secondary Client object needed to speed up the data transfer
    """
    try:
        # Start log
        start = time.time()
        logging.info("Start Function: PRONecL1NewModel({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    
        ###########################################################################
        #                  Define Training and Testing Sets                       #
        ###########################################################################
        #prepare table names
        typeB_A_features = "{}.{}.pronecessity_lvl1_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        typeB_A_features_shortname = "pronecessity_lvl1_trainingset_{}{}".format(country_id, str(period))
        
        #                   Import Data Using BigQuery API                        #
    
        #query_string = "SELECT * FROM `{}`".format(typeB_A_features)
        #logging.info("PRONecL1NewModel: Retrieving Data from BQS- Started")
        #logging.info("SQL: "+ query_string)
        #data = bqclient.query(query_string).result().to_dataframe(bqstorage_client=bqstorageclient)
        
        #                 Import Data Using Google Cloud Storage                  #
        
        # Import light table:
        logging.info("PRONecL1NewModel: Retrieving Data from BQS- Started")
        data = ssu.load_table_to_dataframe(gcp_project, gcp_dataset, typeB_A_features_shortname, bucket_name)
        logging.info("PRONecL1NewModel: Retrieving Data from BQS- Finished")
        
        logging.info("PRONecL1NewModel: Retrieving Data from BQS- Finished")

        # getting size of original dataframe:
        buf_obj = io.StringIO()
        data.info(memory_usage='deep', buf=buf_obj)
        data_info = buf_obj.getvalue()
        logging.info("PRONecL1NewModel: Data retirieved Original Size - {}".format(data_info))
        logging.info("PRONecL1NewModel: CPU used: {}%".format(str(psutil.cpu_percent())))
        logging.info("PRONecL1NewModel: RAM used: {}%".format(psutil.virtual_memory()[2]))
        data = data.fillna(0)

        # transform data to Sparse Matrix:
        logging.info("PRONecL1NewModel: Transform Data to Sparse Matrix - Started")
        data_X = csr_matrix((data['feature_value'].values, (data['customer_idx'].values, data['feature_idx'].values)))
        feature_dict = data[['feature_name', 'feature_idx']].drop_duplicates().reset_index(drop=True).sort_values(by='feature_idx', ascending=True)['feature_name'].tolist()
        logging.info("PRONecL1NewModel: Transform Data to Sparse Matrix - Finished")
        
        ###########################################################################
        #                           K-MEANS Fitting                               #
        ###########################################################################
        if pronec_l1_init == 'PCA':
            pca = PCA(n_components=2, random_state=27218).fit(data_X.todense())
            pronec_l1_init = pca.components_
        
        logging.info("PRONecL1NewModel: Fitting Model - Started")
        start_fit = time.time()
        kmeans_k = KMeans(  n_clusters=2, 
                            init=pronec_l1_init, 
                            random_state=27218, 
                            max_iter=pronec_l1_iter, 
                            n_jobs=-1,
                            precompute_distances='auto')
        kmeans_k.fit(data_X)
        logging.info("PRONecL1NewModel: Fitting Model - Finished || Time: {} sec.".format(str(round(time.time()-start_fit))))

        labels_k = kmeans_k.labels_
        distances = pd.DataFrame(kmeans_k.transform(data_X))

        #Features per Cluster:
        kmeans_results = pd.DataFrame(data_X.todense(), columns=feature_dict)
        kmeans_results['cluster_k'] = labels_k
        kmeans_results['distance_k'] = distances.min(axis=1)
        kmeans_results['algorithm'] = "K-MEANS - init: {}".format(pronec_l1_init)
        #Elements per Cluster:
        k_resume = kmeans_results.groupby(['cluster_k']).count()[['algorithm']]
        k_resume['perc'] = 100*k_resume['algorithm']/ k_resume['algorithm'].sum()
        k_resume.columns = ['Customers', 'Percentage']
        #send info into Log:
        logging.info('PRONecL1NewModel: Results Resume: \n'+(60*'_')+'\n\t\t'+k_resume.round(4).to_string().replace('\n', '\n\t')+'\n'+(60*'_'))
        logging.info('PRONecL1NewModel: Results Properties \n'+(130*'_')+'\n\t\t'+kmeans_results.groupby(['cluster_k']).agg(['mean', 'std']).round(4).to_string().replace('\n', '\n\t') + '\n' + (130*'_'))

        ###########################################################################
        #                             Save Pickle File                            #
        ########################################################################### 
        logging.info('PRONecL1NewModel: Saving Model to Pickle File - Started')
        #define pickle file name and path:
        pickle_directory = "../DATA_MODELS/{}".format(country_id)
        pickle_file = 'pro_necessity_lvl1_kmeans.pickle'
        pickle_path = "{}/{}".format(pickle_directory, pickle_file)
        #check if folder exists:
        if not os.path.exists(pickle_directory):
            os.makedirs(pickle_directory)
        #check if typeB_A pickle file exists:
        if os.path.isfile(pickle_path):
            #if exists, rename it (putting last modifictaion date in front of it)
            old_date = datetime.fromtimestamp(os.path.getmtime(pickle_path))
            old_date = old_date.strftime("%Y%m%d")
            os.rename(pickle_path,"{}/{}_{}".format(pickle_directory, old_date, pickle_file))
        #if exists, rename it:
        pickle_out = open(pickle_path,"wb")
        pickle.dump(kmeans_k, pickle_out)
        pickle_out.close()
        logging.info("PRONecL1NewModel: Model saved in File '{}'".format(pickle_path))
        logging.info('PRONecL1NewModel: Saving Model to Pickle File - Finished')

        ###########################################################################
        #                             Save Pickle File                            #
        ########################################################################### 
        # End 
        logging.info("Finishing Function: PRONecL1NewModel || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def PRONecL1NewScoring(country_id, period, gcp_project, gcp_dataset, bucket_name, bqclient, bqstorageclient, pronecessity_lvl1_dict={0: 'typeB_A', 1:'typeB_B'}):
    """
    PRONecL1NewScoring Function:
    Uses the K-Means's predict function to assign a classification to each Professional Customer.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    pronecessity_lvl1_dict Dict: key and name for the clusters
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Client object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    bqstorageclient obj: BigQuery secondary Client object needed to speed up the data transfer
    """
    try:
        # Start log:
        start = time.time()
        logging.info("Start Function: PRONecL1NewScoring({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        ###########################################################################
        #                     Retrieving Clustering Algorithm                     #
        ###########################################################################        
        #define pickle file name:
        logging.info("PRONecL1NewScoring: Retrieving Model - Started")
        pickle_directory = "../DATA_MODELS/{}".format(country_id)
        pickle_file = 'pro_necessity_lvl1_kmeans.pickle'
        pickle_path = "{}/{}".format(pickle_directory, pickle_file)

        #import model from the last piclke file:
        pickle_file = open(pickle_path, 'rb')
        kmeans_k = pickle.load(pickle_file)
        pickle_file.close()
        logging.info("PRONecL1NewScoring: Retrieving Model - Finished")
        
        ###########################################################################
        #                       Retrieving Data from GCP-BQ                       #
        ###########################################################################
        #Prepare table name and references:
        table_name = "pronecessity_lvl1_scoringset_{}{}".format(country_id,str(period))
        
        ###########################################################################
        #                 Import Data Using Google Cloud Storage                  #
        ###########################################################################
        # Import light table:
        start_aux = time.time()
        logging.info("PRONecL1NewScoring: Retrieving Data from BQS- Started")
        data = ssu.load_table_to_dataframe(gcp_project, gcp_dataset, table_name, bucket_name)
        data = data.fillna(0)
        logging.info("PRONecL1NewScoring: Retrieving Data - Finished || Time: {} sec.".format(str(round(time.time()-start_aux))))
        
        # Filtering Dictionaries:
        feature_dict= data[['feature_name', 'feature_idx']].drop_duplicates().reset_index(drop=True).sort_values(by='feature_idx', ascending=True)['feature_name'].tolist()
        logging.info("PRONecL1NewScoring: Total Features: {}".format(len(feature_dict)))
        customer_dict= data[['customer_id', 'customer_idx']].drop_duplicates().reset_index(drop=True).sort_values(by='customer_idx', ascending=True)
        logging.info("PRONecL1NewScoring: Total Customers: {}".format(customer_dict.shape))

        ###########################################################################
        #                  Reducing DataFrame Size (when needed)                  #
        ###########################################################################
        # getting size of original dataframe:
        buf_obj = io.StringIO()
        data.info(memory_usage='deep', buf=buf_obj)
        data_info = buf_obj.getvalue()
        logging.info("PRONecL1NewScoring: Data retirieved Original Size - {}".format(data_info))

        ###########################################################################
        #                          Format Data to Score                           #
        ###########################################################################
        start_aux = time.time()
        logging.info("PRONecL1NewScoring: Pivoting Data retrieved - Started")
        data_X = csr_matrix((data['feature_value'].values, (data['customer_idx'].values, data['feature_idx'].values)))
        logging.info("PRONecL1NewScoring: Data for scoring: {}".format(data_X.shape))
        logging.info("PRONecL1NewScoring: Pivoting Data retrieved - Finished || Time: {} sec.".format(str(round(time.time()-start_aux))))
        #release memory from the original DataFrame 
        del data

        ###########################################################################
        #                                SCORE DATA                               #
        ###########################################################################
        start_aux = time.time()
        logging.info("PRONecL1NewScoring: Scoring Data - Started")
        result = kmeans_k.predict(data_X)
        logging.info("PRONecL1NewScoring: Scoring Data - Finished || Time: {} sec.".format(str(round(time.time()-start_aux))))

        #format prediction
        pronecessity_lvl1_dict_df = pd.DataFrame.from_dict(pronecessity_lvl1_dict, orient='index', columns=['necessity_lvl_1']).reset_index().rename(columns={'index':'cluster_id'})
        customer_dict['cluster_id'] = result
        result_df =  pd.merge(customer_dict, pronecessity_lvl1_dict_df, on='cluster_id', how='inner')

        #Add id values
        result_df['country_id'] = country_id
        result_df['country_id'] = result_df['country_id'].astype('str')
        result_df['model'] = 'K-means'
        class_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result_df['classification_dttm'] = class_date
        result_df['period'] = period
        #rearrange columns to match create table:
        result_df = result_df[["country_id","model","classification_dttm","period","customer_idx","customer_id","cluster_id","necessity_lvl_1"]]
        result_df = result_df.set_index('customer_idx')

        #Elements per Cluster:
        k_resume = result_df.groupby(['necessity_lvl_1']).count()[['customer_id']]
        k_resume['perc'] = 100*k_resume['customer_id']/ k_resume['customer_id'].sum()
        k_resume.columns = ['Customers', 'Percentage']
        #send info into Log:
        logging.info('\n\t PRONecL1NewModel: Results Resume: \n'+(60*'_')+'\n\t'+k_resume.round(4).to_string().replace('\n', '\n\t')+'\n'+(60*'_'))

        ###########################################################################
        #                          Saving Model Results                           #
        ###########################################################################
        logging.info('PRONecL1NewScoring: Saving Results - Started')

        #Define table names:
        result_table = "{}.{}.pronecessity_lvl1_profile_results_{}{}".format(gcp_project,gcp_dataset, country_id, period)
        all_customers_table = "{}.{}.pronecessity_lvl1_all_customers_{}{}".format(gcp_project,gcp_dataset, country_id, period)
        
        #drop table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(result_table)
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
                customer_id STRING, 
                cluster_id INT64,
                necessity_lvl_1 STRING            
            );
        """.format(result_table)
        logging.info("SQL: {} - ".format(query_create))
        bqclient.query(query_create).result()

        #load dataframe into table:
        logging.info('PRONecL1NewScoring: Loading Data from Dataframe - Started')
        job = bqclient.load_table_from_dataframe(result_df, result_table)
        job.result()  # Waits for table load to complete.
        assert job.state == "DONE"
        logging.info('PRONecL1NewScoring: Loading Data from Dataframe - Finished')
        
        #get typeB_A cluster id:
        typeB_A_key = 0
        for key, val in pronecessity_lvl1_dict.items():
            if val == 'typeB_A':
                typeB_A_key = key

        #Insert Clients without Variables:
        query_insert = """
                        INSERT INTO `{0:s}` 
                        SELECT 
                          -1 as customer_idx
                          , '{1:s}' as country_id
                          , 'K-means' as model
                          , '{2:s}' as classification_dttm
                          , {3:s} as period
                          , A.customer_id
                          , {4:s} as cluster_id
                          , 'typeB_A' necessity_lvl_1
                        FROM `{5:s}` A
                        LEFT JOIN `{0:s}` B ON A.customer_id = B.customer_id
                        WHERE B.customer_id IS NULL
        """.format(result_table, country_id, class_date, str(period), str(typeB_A_key), all_customers_table)
        
        #Excecute Insert:
        logging.info('PRONecL1NewScoring: Insert Customers with no Features - Started')
        logging.info("SQL: {} - ".format(query_insert))
        bqclient.query(query_insert).result()
        logging.info('PRONecL1NewScoring: Insert Customers with no Features - Finished')

        # End 
        logging.info("Finishing Function: PRONecL1NewScoring || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

###############################################################################
# typeB by Necessity: Level 2 (L2A, L2B, L2C)                                 #
###############################################################################
def PRONecL2NewModel(country_id, period, gcp_project, gcp_dataset, bqclient, bqstorageclient, pronec_l2_init, pronec_l2_iter, bucket_name):
    """
    PRONecL2NewScoring Function:
    Uses the K-Means's predict function to assign a classification to each Professional Customer.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Client object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    bqstorageclient obj: BigQuery secondary Client object needed to speed up the data transfer
    """
    try:
        n_clusters = 4
        # Start log
        start = time.time()
        logging.info("Start Function: PRONecL2NewModel({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    
        ###########################################################################
        #                  Define Training and Testing Sets                       #
        ###########################################################################
        #prepare table names
        lvl2_features = "{}.{}.pronecessity_lvl2_trainingset_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        lvl2_features_shortname = "pronecessity_lvl2_trainingset_{}{}".format(country_id, str(period))

        ###########################################################################
        #                 Import Data Using Google Cloud Storage                  #
        ###########################################################################
        # Import light table:
        start_aux = time.time()
        logging.info("PRONecL2NewModel: Retrieving Data from BQS- Started")
        data = ssu.load_table_to_dataframe(gcp_project, gcp_dataset, lvl2_features_shortname, bucket_name)
        logging.info("PRONecL2NewModel: Retrieving Data - Finished || Time: {} sec.".format(str(round(time.time()-start_aux))))

        # getting size of original dataframe:
        buf_obj = io.StringIO()
        data.info(memory_usage='deep', buf=buf_obj)
        data_info = buf_obj.getvalue()
        logging.info("PRONecL2NewModel: Data retirieved Original Size - {}".format(data_info))
        logging.info("PRONecL2NewModel: CPU used: {}%".format(str(psutil.cpu_percent())))
        logging.info("PRONecL2NewModel: RAM used: {}%".format(psutil.virtual_memory()[2]))
        data = data.fillna(0)

        # transform data to Sparse Matrix:
        logging.info("PRONecL2NewModel: Transform Data to Sparse Matrix - Started")
        data_X = csr_matrix((data['feature_value'].values, (data['customer_idx'].values, data['feature_idx'].values)))
        feature_dict = data[['feature_name', 'feature_idx']].drop_duplicates().reset_index(drop=True).sort_values(by='feature_idx', ascending=True)['feature_name'].tolist()
        logging.info("PRONecL2NewModel: Transform Data to Sparse Matrix - Finished")

        ###########################################################################
        #                           K-MEANS Fitting                               #
        ###########################################################################
        np.random.seed(27218)
        if pronec_l2_init == 'PCA':
            pca = PCA(n_components=3).fit(data_X.todense())
            pronec_l2_init = pca.components_

        logging.info("PRONecL2NewModel: Fitting Models - Started")
        start_fit = time.time()
        kmeans_k = KMeans(  n_clusters=n_clusters, 
                            init=pronec_l2_init, 
                            random_state=27218, 
                            max_iter=pronec_l2_iter, 
                            n_jobs=-1,
                            precompute_distances='auto')
        start_km = time.time()
        logging.info("PRONecL2NewModel: K-Means Model - Fitting Started")
        kmeans_k.fit(data_X)
        logging.info("PRONecL2NewModel: K-Means Model - Fitting Finished || Time:{} sec.".format(str(round(time.time()-start_km))))
        labels_k = kmeans_k.labels_

        ###########################################################################
        #                           K-MEANS: Save Plot                            #
        ###########################################################################
        logging.info("PRONecL2NewModel: K-Means Model - Saving image")
        #get index features from dictionary
        x_index = feature_dict.index('per_trx_frec_N0')
        y_index = feature_dict.index('total_sales_amt')

        #Declare DataFrame to filter indexs of each cluster member
        df_labels_k = pd.DataFrame(labels_k, columns=['labels_k'])
        df_labels_k['index_label'] = df_labels_k.index

        fig = plt.figure(figsize=(8, 8))
        #for each cluster:
        for clust in range(n_clusters):
            plt.scatter(data_X.transpose().todense()[x_index, df_labels_k[df_labels_k['labels_k']==clust]['index_label'].tolist()].tolist()[0], 
                        data_X.transpose().todense()[y_index, df_labels_k[df_labels_k['labels_k']==clust]['index_label'].tolist()].tolist()[0],
                        label='Cluster {}'.format(str(clust)), 
                        alpha=0.8
                        )
        plt.xlabel('Speciality Level', fontsize=10)
        plt.ylabel('Total Sales Amount (24 Months)', fontsize=10)
        plt.title('PRO Customers: typeB_B: {} clusters'.format(n_clusters))
        plt.legend()
        plt.savefig('../DATA_MODELS/{0:s}/pro_necessity_lvl2_kmeans_k{1:s}.png'.format(country_id,str(n_clusters)))
        #plt.show()

        ###########################################################################
        #                          Decision Tree Fitting                          #
        ###########################################################################
        # First retrieve the real values of each customer's feature:
        # Standarization was in place Calculate Threshold's Real Values:
        if ssu.bqCheckTableExists('pronecessity_lvl2_standardization_values_{}'.format(country_id), gcp_project, gcp_dataset, bqclient):
            features_dict_std = ssu.load_table_to_dataframe(gcp_project, gcp_dataset, 'pronecessity_lvl2_standardization_values_{}'.format(country_id) , bucket_name)
            features_dict_idx = ssu.load_table_to_dataframe(gcp_project, gcp_dataset, 'pronecessity_lvl2_features_dictionary_{}'.format(country_id) , bucket_name)
            
            for feat in features_dict_std.feature_name:
                f_idx = features_dict_idx[features_dict_idx['feature_name']==feat]['feature_idx'].values[0]
                if features_dict_std[features_dict_std['feature_name']==feat]['std_type'].values == 'MinMax':
                    maxx = features_dict_std[features_dict_std['feature_name']==feat]['max_value'].values[0]
                    minx = features_dict_std[features_dict_std['feature_name']==feat]['min_value'].values[0]
                    data_X.data[data_X.indices == f_idx] = ((maxx - minx)*data_X.data[data_X.indices == f_idx])+minx
                else:
                    stdx = features_dict_std[features_dict_std['feature_name']==feat]['std_dev_feature'].values[0]
                    meanx = features_dict_std[features_dict_std['feature_name']==feat]['avg_feature'].values[0]
                    data_X.data[data_X.indices == f_idx] = (stdx*data_X.data[data_X.indices == f_idx])+meanx
        
        # Then train the tree
        tree_clf = tree.DecisionTreeClassifier(max_depth=2, random_state=27218)

        logging.info("PRONecL2NewModel: Decision Tree Model - Fitting")
        start_tree = time.time()
        tree_clf = tree_clf.fit(data_X, labels_k) 
        logging.info("PRONecL2NewModel: Fitting Models - Finished || Time: {} sec.".format(str(round(time.time()-start_tree))))

        dot_data = tree.export_graphviz(tree_clf, 
                                        out_file=None, 
                                        feature_names=feature_dict,  
                                        filled=True, 
                                        rounded=True,  
                                        special_characters=True
                                       )
        graph = graphviz.Source(dot_data)  
        graph.render('../DATA_MODELS/{0:s}/pro_necessity_lvl2_Dtree'.format(country_id,n_clusters), format='png', cleanup=True)

        ###########################################################################
        #                             Resume Results                              #
        ########################################################################### 
        labels_tree = tree_clf.predict(data_X)

        #Features per Cluster:
        tree_results = pd.DataFrame(data_X.todense(), columns=feature_dict)
        tree_results['cluster_tree'] = labels_tree
        tree_results['algorithm'] = "{}".format(pronec_l2_init)
        #Elements per Cluster:
        tree_resume = tree_results.groupby(['cluster_tree']).count()[['algorithm']]
        tree_resume['perc'] = 100*tree_resume['algorithm']/ tree_resume['algorithm'].sum()
        tree_resume.columns = ['Customers', 'Percentage']
        #send info into Log:
        logging.info('PRONecL2NewModel: Results Resume: \n'+(60*'_')+'\n\t'+tree_resume.round(4).to_string().replace('\n', '\n\t')+'\n'+(60*'_'))

        ###########################################################################
        #                                Save Rules                               #
        ########################################################################### 
        thresholds = ssu.extract_thresholds(0, tree_clf.tree_, feature_dict, thresholds={})
        logging.info('PRONecL2NewModel: Thresholds from the Tree: {}'.format(thresholds))

        ###########################################################################
        #                            Save Pickle Files                            #
        ########################################################################### 
        # Kmeans
        logging.info('PRONecL2NewModel: Saving Model to Pickle File - Started')
        # define pickle file name and path:
        pickle_directory = "../DATA_MODELS/{}".format(country_id)
        pickle_kmeans_file = 'pro_necessity_lvl2_kmeans_model.pickle'
        pickle_dtree_file = 'pro_necessity_lvl2_Dtree_model.pickle'
        pickle_threshold_file = 'pro_necessity_lvl2_threshold_dict.pickle'

        pickle_kmeans_path = "{}/{}".format(pickle_directory, pickle_kmeans_file)
        pickle_dtree_path = "{}/{}".format(pickle_directory, pickle_dtree_file)
        pickle_threshold_path = "{}/{}".format(pickle_directory, pickle_threshold_file)
        # check if folder exists:
        if not os.path.exists(pickle_directory):
            os.makedirs(pickle_directory)

        # check if Kmeans pickle file exists:
        if os.path.isfile(pickle_kmeans_path):
            # if exists, rename it (putting last modifictaion date in front of it)
            old_date = datetime.fromtimestamp(os.path.getmtime(pickle_kmeans_path))
            old_date = old_date.strftime("%Y%m%d")
            os.rename(pickle_kmeans_path,"{}/{}_{}".format(pickle_directory, old_date, pickle_kmeans_file))

        # check if DTree pickle file exists:
        if os.path.isfile(pickle_dtree_path):
            # if exists, rename it (putting last modifictaion date in front of it)
            old_date = datetime.fromtimestamp(os.path.getmtime(pickle_dtree_path))
            old_date = old_date.strftime("%Y%m%d")
            os.rename(pickle_dtree_path,"{}/{}_{}".format(pickle_directory, old_date, pickle_dtree_file)) 
            
        # check if Threshold pickle file exists:
        if os.path.isfile(pickle_threshold_path):
            # if exists, rename it (putting last modifictaion date in front of it)
            old_date = datetime.fromtimestamp(os.path.getmtime(pickle_threshold_path))
            old_date = old_date.strftime("%Y%m%d")
            os.rename(pickle_threshold_path,"{}/{}_{}".format(pickle_directory, old_date, pickle_threshold_file)) 

        # save new files:
        pickle_out = open(pickle_kmeans_path,"wb")
        pickle.dump(kmeans_k, pickle_out)
        pickle_out.close()
        logging.info("PRONecL2NewModel: Model saved in File '{}'".format(pickle_kmeans_file))
        
        pickle_out = open(pickle_dtree_path,"wb")
        pickle.dump(tree_clf, pickle_out)
        pickle_out.close()
        logging.info("PRONecL2NewModel: Model saved in File '{}'".format(pickle_dtree_file))
        
        pickle_out = open(pickle_threshold_path,"wb")
        pickle.dump(thresholds, pickle_out)
        pickle_out.close()
        logging.info("PRONecL2NewModel: Model saved in File '{}'".format(pickle_threshold_file))
        
        logging.info('PRONecL2NewModel: Saving Model to Pickle File - Finished')
        logging.info("Finishing Function: PRONecL2NewModel || Time: {} sec.".format(str(round(time.time()-start))))
        
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

def PRONecL2NewScoring(country_id, period, gcp_project, gcp_dataset, bqclient, bqstorageclient, pronec_l2_outliers_rules):
    """
    PRONecL2NewScoring Function:
    Uses the K-Means's predict function to assign a classification to each Professional Customer.

    country_id str: Country to process
    period int: Period to process. FORMAT YYYYMM
    pronecessity_lvl2_dict Dict: key and name for the clusters
    gcp_project str: GCP project to execute the process
    gcp_dataset str: GCP dataset to store the process
    bqclient obj: BigQuery Client object needed to connect and execute 
        querys and statements in Google Cloud platform BigQuery environment
    bqstorageclient obj: BigQuery secondary Client object needed to speed up the data transfer
    """
    try:
        # Start log:
        start = time.time()
        logging.info("Start Function: PRONecL2NewScoring({}, {}, {}, {})".format(country_id, period, gcp_project, gcp_dataset))    

        ###########################################################################
        #                     Retrieving Clustering Algorithm                     #
        ###########################################################################        
        #define pickle file name:
        logging.info("PRONecL2NewScoring: Retrieving Thresholds - Started")
        pickle_directory = "../DATA_MODELS/{}".format(country_id)
        pickle_file = 'pro_necessity_lvl2_threshold_dict.pickle'
        pickle_path = "{}/{}".format(pickle_directory, pickle_file)

        #import model from the last piclke file:
        pickle_file = open(pickle_path, 'rb')
        thresholds = pickle.load(pickle_file)
        pickle_file.close()
        logging.info("PRONecL2NewScoring: Retrieving Thresholds - Finished")

        ###########################################################################
        #                                SCORE DATA                               #
        ###########################################################################
        #Define table names:
        result_table = "{}.{}.pronecessity_lvl2_profile_results_{}{}".format(gcp_project,gcp_dataset, country_id, period)
        features_table   = "{}.{}.segmentation_features_{}{}".format(gcp_project, gcp_dataset, country_id, str(period))
        all_customers_table = "{}.{}.pronecessity_lvl2_all_customers_{}{}".format(gcp_project,gcp_dataset, country_id, period)
        result_prev_table =  "{}.{}.segmentation_profile_results_{}{}".format(gcp_project,gcp_dataset, country_id, period)

        #Add id values
        model = 'K-means - Decision Tree'
        class_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")        

        #drop table if exists:
        query_drop = "DROP TABLE IF EXISTS `{}`".format(result_table)
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
                customer_id STRING, 
                cluster_id INT64,
                necessity_lvl_2 STRING,       
                necessity_lvl_2_recency STRING
            );
        """.format(result_table)
        logging.info("SQL: {} - ".format(query_create))
        bqclient.query(query_create).result()

        #L2A:
        query_insert =  """ 
            INSERT INTO `{0:s}` 
            SELECT '{1:s}' as country_id
                ,  '{2:s}' as model
                ,  '{3:s}' as classification_dttm
                ,  {4:s} as period
                ,  ac.customer_id
                ,  0 as cluster_id
                ,  'L2A' as necessity_lvl_2
                , '' as necessity_lvl_2_recency
            FROM `{5:s}` ac
            INNER JOIN `{6:s}` ft ON ac.customer_id = ft.customer_id
            LEFT JOIN `{0:s}` re ON  ac.customer_id = re.customer_id
            WHERE re.customer_id IS NULL
            AND (ft.feature_name  = 'total_sales_amt' AND ft.feature_value > {7:s})
        """.format(result_table,
                   country_id, 
                   model, 
                   class_date,
                   str(period),
                   all_customers_table,
                   features_table,
                   str(thresholds['total_sales_amt']))

        logging.info("SQL: {} - ".format(query_insert))
        bqclient.query(query_insert).result()

        #L2B:
        query_insert =  """ 
            INSERT INTO `{0:s}` 
            SELECT '{1:s}' as country_id
                ,  '{2:s}' as model
                ,  '{3:s}' as classification_dttm
                ,  {4:s} as period
                ,  ac.customer_id
                ,  1 as cluster_id
                ,  'L2B' as necessity_lvl_2
                , '' as necessity_lvl_2_recency
            FROM `{5:s}` ac
            INNER JOIN `{6:s}` ft ON ac.customer_id = ft.customer_id
            LEFT JOIN `{0:s}` re ON  ac.customer_id = re.customer_id
            WHERE re.customer_id IS NULL
            AND (ft.feature_name  = 'per_trx_frec_N0' AND ft.feature_value > {7:s})
            AND ac.customer_id in (
                                    SELECT distinct customer_id 
                                    FROM `{6:s}`
                                    WHERE (feature_name  = 'total_transactions' AND feature_value >= {8:s})
                                )
        """.format(result_table,
                   country_id, 
                   model, 
                   class_date,
                   str(period),
                   all_customers_table,
                   features_table,
                   str(thresholds['per_trx_frec_N0']),
                   str(pronec_l2_outliers_rules['min_trx'])
                  )

        logging.info("SQL: {} - ".format(query_insert))
        bqclient.query(query_insert).result()  

        #L2C:
        query_insert =  """ 
            INSERT INTO `{0:s}` 
            SELECT '{1:s}' as country_id
                ,  '{2:s}' as model
                ,  '{3:s}' as classification_dttm
                ,  {4:s} as period
                ,  ac.customer_id
                ,  2 as cluster_id        
                ,  'L2C' as necessity_lvl_2
                , '' as necessity_lvl_2_recency
            FROM `{5:s}` ac
            LEFT JOIN `{0:s}` re ON  ac.customer_id = re.customer_id
            WHERE re.customer_id IS NULL
        """.format(result_table,
                   country_id, 
                   model, 
                   class_date,
                   str(period),
                   all_customers_table,
                   features_table)

        logging.info("SQL: {} - ".format(query_insert))
        bqclient.query(query_insert).result() 

        ###########################################################################
        #                              Business Rules                             #
        ########################################################################### 
        #The business Rules are definitions that overwrite the result of the models score process:
        #
        # Business Rule - SII CONT (BR - SII L2A): Any customer with records in the SII (Servicio de impuestos Internos) 
        # with a sales tranche greater than or equal to the one defined by the user and with at least one transaction in history 
        # will be considered "L2A" in th next step, ergo needs to be mark as PROFESSIONAL. Overwrite previous rules.
        #############################################################################
        query_update =  """ 
            UPDATE `{0:s}` 
            SET necessity_lvl_2 = 'L2A', model = CONCAT(model, ' - BR SSII')
            WHERE customer_id  in (SELECT customer_id FROM `{1:s}` WHERE model like '%L2A%')
        """.format(result_table, 
                   result_prev_table)

        logging.info("SQL: {} - ".format(query_update))
        bqclient.query(query_update).result() 
        
        #RECENCY RULES:
        query_update =  """ 
            UPDATE `{0:s}` 
            SET necessity_lvl_2_recency = 'NO FUGADO'
            WHERE customer_id in (
                                SELECT DISTINCT re.customer_id
                                FROM `{0:s}` re
                                INNER JOIN `{1:s}` ft ON re.customer_id = ft.customer_id
                                WHERE ( ft.feature_name  = 'months_since_last_trx' 
                                        AND ft.feature_value <= {2:s} )
                                )
        """.format(result_table,
                   features_table,
                   str(thresholds['months_since_last_trx']))

        logging.info("SQL: {} - ".format(query_update))
        bqclient.query(query_update).result() 

        query_update =  """ 
            UPDATE `{0:s}` 
            SET necessity_lvl_2_recency = 'FUGADO'
            WHERE necessity_lvl_2_recency = ''
        """.format(result_table)

        logging.info("SQL: {} - ".format(query_update))
        bqclient.query(query_update).result() 

        # End 
        logging.info("Finishing Function: PRONecL2NewScoring || Time: {} sec.".format(str(round(time.time()-start))))
    except Exception as e:
        logging.exception('Error occurred: ' + str(e))
        raise

