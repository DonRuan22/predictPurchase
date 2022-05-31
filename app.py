from flask import Flask, request
import json
import logging
import os
from google.cloud import bigquery
import joblib
import pandas as pd
import numpy as np
import xgboost as xgb
import gcsfs


app = Flask(__name__)


def getPrediction(dataUser):
    filename = 'gcs://don-onlineretail/predict_purchase_model.joblib.pkl'
    fs = gcsfs.GCSFileSystem()
    logging.warning(dataUser) 
    with fs.open(filename, 'rb') as f:
        model = joblib.load(f)
        cols_when_model_builds = model.get_booster().feature_names
        for each in cols_when_model_builds:
            if each not in dataUser.columns:
                dataUser[each] = 0
        predicted = model.predict(dataUser)
        return predicted[0]


def loadCustomerData(customerId):
    bigqueryClient = bigquery.Client()
    query_string = f"SELECT \
    * \
    FROM `donexp.onlineRetail.onlineRetailTransformed` \
    WHERE CustomerID = {customerId}"
    tx_class = (
        bigqueryClient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
        
    tx_classdb = pd.get_dummies(tx_class)
    #tx_classdb['Recency'] = tx_classdb['Recency'].astype('int64')
    #tx_classdb['RecencyCluster'] = tx_classdb['RecencyCluster'].astype('int64')
    #tx_classdb['Frequency'] = tx_classdb['Frequency'].astype('int64')
    #tx_classdb['FrequencyCluster'] = tx_classdb['FrequencyCluster'].astype('int64')
    #tx_classdb['RevenueCluster'] = tx_classdb['RevenueCluster'].astype('int64')
    #tx_classdb['OverallScore'] = tx_classdb['OverallScore'].astype('int64')
    tx_classdb['NextPurchaseDayRange'] = 2
    tx_classdb.loc[tx_classdb.NextPurchaseDay>20,'NextPurchaseDayRange'] = 1
    tx_classdb.loc[tx_classdb.NextPurchaseDay>50,'NextPurchaseDayRange'] = 0
        
    logging.warning('dataframe head - {}'.format(tx_classdb.describe()))  
    logging.warning('dataframe head - {}'.format(tx_classdb.NextPurchaseDayRange))  
    logging.warning('dataframe head - {}'.format(tx_classdb.NextPurchaseDay))  
    logging.warning(tx_classdb.dtypes) 
    

    #train & test split
    tx_classdb = tx_classdb.drop('NextPurchaseDay',axis=1)
    X = tx_classdb.drop('NextPurchaseDayRange',axis=1)
    
    return X



#Function to access the Sender API
def callSendAPI(customerId, response):
    #PAGE_ACCESS_TOKEN = config.PAGE_ACCESS_TOKEN
    logging.warning("Response: "+ str(response)) 
    payload = {'customerid': customerId,**response}   
    
    logging.warning(payload) 
    return payload



#Function for handling a message from MESSENGER
def handleMessage(customerId):
    print("handle message")
    #callSendAPI(senderPsid, "","sender_action")
    data_customer = loadCustomerData(customerId)
    if(data_customer.shape[0] > 0):
        predicted_range = getPrediction(data_customer)
        response = {"predictedRange":predicted_range}
    else:
        response = {"error":"CustomerId not exist"}
    message = callSendAPI(response)
    return message
    

@app.route('/', methods=["GET", "POST"])
def home():

    return 'HOME'

@app.route('/webhooks/predict_purchase/webhook', methods=["POST"])
def index():
    if request.method == 'POST':
        data = request.data
        body = json.loads(data.decode('utf-8'))
        logging.warning("Body: "+ str(data)) 
        

        if 'customerId' in body:
            customerId = body['customerId']
            response = handleMessage(customerId)

            return response, 200
        else:
            return 'ERROR', 404



if __name__ == '__main__':
    app.run(host='0.0.0.0',port=int(os.environ.get("PORT", 5005)), debug=True)