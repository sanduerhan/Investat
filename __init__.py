import logging
import azure.functions as func
from estado import estado_function
from administratiu import administratiu_function
from empresarial import empresarial_function
from SS import ss_function
from datetime import datetime
import json
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
import csv
import os
import pandas as pd
import re

connectionString = "DefaultEndpointsProtocol=https;AccountName=investat;AccountKey=Wz8yLke0HUY2CemED6MjEn/osk+BAre+7xKgmHW3XNbuk+bMozjcaQBcsU1r1nJUEGe2ccDuNo/7+AStEWz+4w==;EndpointSuffix=core.windows.net"
containerName = "contenedorinversionsestat"
Arxius_processats=[]
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="estado", auth_level=func.AuthLevel.ANONYMOUS)
def main_estado(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Estado HTTP trigger.')

    return estado_function(req)
'''
@app.route(route="administratiu/{Year_global}", methods=["GET"])
@app.blob_input(
    arg_name="input_blob",
    path="administratiu-container/{Year_global}/path/to/input_blob.csv",
    connection="AzureWebJobsStorage"
)
@app.blob_output(
    arg_name="output_blob",
    path="administratiu-container/{Year_global}/path/to/output_blob.csv",
    connection="AzureWebJobsStorage"
)
def main_administratiu(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Administratiu HTTP trigger.')

    return administratiu_function(req)
@app.route(route="sector_empresarial/{Year_global}", methods=["GET"])
@app.blob_input(
    arg_name="input_blob",
    path="empresarial-container/{Year_global}/path/to/input_blob.csv",
    connection="AzureWebJobsStorage"
)
@app.blob_output(
    arg_name="output_blob",
    path="empresarial-container/{Year_global}/path/to/output_blob.csv",
    connection="AzureWebJobsStorage"
)
def main_empresarial(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Empresarial HTTP trigger.')
    
    return empresarial_function(req)
@app.route(route="ss/{Year_global}", methods=["GET"])
@app.blob_input(
    arg_name="input_blob",
    path="ss-container/{Year_global}/path/to/input_blob.csv",
    connection="AzureWebJobsStorage"
)
@app.blob_output(
    arg_name="output_blob",
    path="ss-container/{Year_global}/path/to/output_blob.csv",
    connection="AzureWebJobsStorage"
)
def main_ss(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('SS HTTP trigger.')
    return ss_function(req)
    '''

@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )