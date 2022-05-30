from flask import Flask, request
import requests
import json
import config
import logging
import os
from google.cloud import bigquery
import gcsfs
import joblib
import pandas as pd
import numpy as np
import xgboost as xgb



app = Flask(__name__)
SECRET_KEY = 'b8d43ce14d05828c73257013c8e67b95'
#PAGE_ACCESS_TOKEN = "EAACCGdwRfhABANBdZCtGQWTBPWLiH2wnRLreO6vZAtP6WZBvTAsmDkCVYkVD7fmUtGu5ARlGtI1tV8nhSyZCjy0sHGfKRNZAJcemHpaQ0glcfqITZBxuZA6Y6RrehcrgWvZCzAWVT9T3Rln5lOMArSy9A64HPOg19AT9T2PyWWdZAQdeFTuESLpqS"
VERIFY_TOKEN = 'rasa-don'
global INIT_VARI
global mId 
mId = []
INIT_VARI=''


def getPrediction(dataUser):
    model_dir="gcs://don-onlineretail/"
    model_path = os.path.join(model_dir, 'predict_purchase_model.joblib.pkl')
    model = joblib.load(model_path)
    predicted = model.predict(dataUser[0])
    return predicted


def loadCustomerData(customerId):
    bigqueryClient = bigquery.Client()
    query_string = """
    SELECT
    *
    FROM `donexp.onlineRetail.onlineRetailTransformed`
    WHERE CustomerID = {customerId}
    """
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
    
    return tx_classdb



#Function to access the Sender API
def callSendAPI(customerId, response, type_response='message'):
    #PAGE_ACCESS_TOKEN = config.PAGE_ACCESS_TOKEN
    logging.warning("Response: "+ str(response)) 
        payload = {
        'customerid': customerId,
        response
        }   
    headers = {'content-type': 'application/json'}

    
    logging.warning(r.request.headers) 
    logging.warning(r.url) 
    logging.warning(r.content) 
    logging.warning(r.__dict__) 
    return payload



#Function for handling a message from MESSENGER
def handleMessage(customerId, receivedMessage):
    print("handle message")
    #callSendAPI(senderPsid, "","sender_action")
    data_customer = loadCustomerData(customerId)
    if(data_customer.shape[0] > 0):
        predicted_range = getPrediction(data_customer)
        response = {"customerId":customerId, "predictedRange":predicted_range}
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