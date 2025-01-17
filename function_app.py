import logging
import azure.functions as func
from functions.estado import estado_function
from functions.administratiu import administratiu_function
from functions.empresarial import empresarial_function
from functions.SS import ss_function
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
import json
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
import csv
import os
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.authentication_context import AuthenticationContext
import re

connectionString = os.environ['CUSTOMCONNSTR_storage']
containerName = "contenedorinversionsestat"
Arxius_processats=[]
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="estado/{Year_global}", methods=["GET"])
@app.blob_input(
    arg_name="input_blob",
    path="estado-container/{Year_global}/path/to/input_blob.csv",
    connection="AzureWebJobsStorage"
)
@app.blob_output(
    arg_name="output_blob",
    path="estado-container/{Year_global}/path/to/output_blob.csv",
    connection="AzureWebJobsStorage"
)
def main_estado(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Estado HTTP trigger.')

    return estado_function(req)

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