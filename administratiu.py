import azure.functions as func
import utils
from utils import descarga_blob, subida_blob, individual, individual_SS
from datetime import datetime
import json
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
import csv
import os
import pandas as pd
import re


def administratiu_function(req: func.HttpRequest, outputBlob: func.Out[str]) -> func.HttpResponse:
    Year_global = int(req.route_params['Year_global'])
    llista_final_ccaa = []

    for nom_arxiu in ['Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/3.Sector Publico Administrativo']:
        llista_origen = descarga_blob(nom_arxiu)
        llista_final = []
        comunitat = ""
        provincia = ""
        entidad = ""
        for row in llista_origen:
            if len(row) != 0 and "PROVINCIA" in row[0]:
                # Process data as per logic
                pass
            if len(row) != 0 and row[0].strip().isdigit():
                toappend = []
                # Collect data into list
                llista_final.append(toappend)

        # Append the processed data
        llista_final_ccaa.extend(llista_final)

    # Define the headers for the processed data
    capcelera = ['COMUNITAT_AUTONOMA', 'PROVINCIA', 'ENTITAT', 'CODI PROJECTE', 'DENOMINACIO', 'COST TOTAL', 'INICI', 'FI','TIPUS', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
    llista_final_ccaa.insert(0, capcelera)
    
    # Upload processed data to Blob Storage
    upload_file = 'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/3.Sector Publico Administrativo/'+ str(Year_global) + "_PRES_FACT_DET_SP_ADMIN.csv"
    subida_blob(upload_file, llista_final_ccaa)

    return func.HttpResponse("Sector Administratiu carregat", status_code=200)