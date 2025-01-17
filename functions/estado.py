import azure.functions as func
from functions.utils import descarga_blob, subida_blob, individual, individual_SS
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

def estado_function(req: func.HttpRequest) -> func.HttpResponse:
    # Fetch the year from the request URL
    Year_global = int(req.route_params['Year_global'])
    
    # Initialize list to store results
    llista_final = []

    # Fetch files from Blob storage
    for nom_arxiu in ['Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/1.Estado']:
        fitxer_estado = descarga_blob(nom_arxiu)
        llista_final = individual(fitxer_estado, llista_final)

    # Define the headers for the data
    capcelera = ['ID_MINISTERI','DESC_MINISTERI' ,'COMUNITAT_AUTONOMA', 'CODI_CENTRE','ID_PROGRAMA', 'ID_ARTICLE',
                 'DESC_CENTRE','ID_PROJECTE', 'NOM_PROJECTE', 'ANY_INICI', 'ANY_FI', 'PROVINCIA', 'TIPUS', 'COST_TOTAL', 'ANY_ANTERIOR', 'ANY_ACTUAL',
                 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']

    # Insert header into the final data
    llista_final.insert(0, capcelera)
    
    # Upload processed data to Blob Storage
    upload_file = 'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/1.Estado/'+ str(Year_global)+ "_PRES_FACT_DET_EST_OOAA_RE.csv"
    subida_blob(upload_file, llista_final)

    return func.HttpResponse("Fitxer d'estado carregat", status_code=200)