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

Arxius_processats=[]

def ss_function(req: func.HttpRequest) -> func.HttpResponse:
    try:
        Year_global = int(req.route_params['Year_global'])
        ruta_arxius = f'Inversions de lestat/{Year_global}/Inversio pressupostada/2.Seguretat Social/'
        llistafinal = []
        llistaentitat = []

        # Extract the last two digits of the year
        year_YY = Year_global - 2000

        header = ['Organisme', 'Programa', 'Article', 'Centre Tipus', 'Número', 'Denominació', 'Inici', 'Final',
                  'Import Total', 'ANY-1', 'ANY_EXC_PRESSUPOSTARI', 'ANY+1', 'ANY+2', 'Província', 'ID Entitat']
        headerentitat = ['ID Entitat', 'Entitat']

        llistaentitat.insert(0, headerentitat)
        llistafinal.insert(0, header)

        # Process files in Arxius_processats
        for nom_arxiu in Arxius_processats:
            if f'{Year_global}/Inversio pressupostada/2.Seguretat Social' in nom_arxiu:
                provincia = nom_arxiu.split('/')[-1][6:-4]
                llista_provincia = descarga_blob(nom_arxiu)
                individual_SS(llista_provincia, llistafinal, provincia, llistaentitat)

        # Upload processed files to blob storage
        upload_file = f'{ruta_arxius}{Year_global}_PRES_FACT_DET_SEGURETAT_SOCIAL.csv'
        subida_blob(upload_file, llistafinal)

        upload_file = f'{ruta_arxius}{Year_global}_DIM_DET_SEGURETAT_SOCIAL_ENTITATS.csv'
        subida_blob(upload_file, llistaentitat)

        return func.HttpResponse("Blob SS subido", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"Error processing SS function: {str(e)}", status_code=500)