import azure.functions as func
import utils
from utils import descarga_blob, subida_blob, individual, individual_SS
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

def empresarial_function(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Get the year from the route parameter
        Year_global = int(req.route_params['Year_global'])

        # Initialize the final list for data aggregation
        llista_final_ccaa = []

        # Process files for the specified year and sector
        for nom_arxiu in ['Inversions de lestat/' + str(Year_global) + '/Inversio pressupostada/4.Sector Publico Empresarial']:
            llista_origen = descarga_blob(nom_arxiu)
            llista_final = []
            comunitat = ""
            provincia = ""
            entidad = ""

            # Process rows in the downloaded blob
            for row in llista_origen:
                if len(row) != 0 and "PROVINCIA" in row[0]:
                    posiciones_dos_puntos = [i for i, char in enumerate(row[0]) if char == ":"]
                    posicion_pro = [i for i in range(len(row[0])) if row[0].startswith("PRO", i)]
                    provincia = row[0][posiciones_dos_puntos[1] + 2:]
                    comunitat = row[0][posiciones_dos_puntos[0] + 2:posicion_pro[0]]

                if len(row) >= 3 and "PROVINCIA" in row[3]:
                    posiciones_dos_puntos = [i for i, char in enumerate(row[3]) if char == ":"]
                    posicion_pro = [i for i in range(len(row[3])) if row[3].startswith("PRO", i)]
                    provincia = row[3][posiciones_dos_puntos[1] + 2:]
                    comunitat = row[3][posiciones_dos_puntos[0] + 2:posicion_pro[0]]

                if len(row) > 2 and "ENTIDAD" in row[2]:
                    aux = row[2].split(":")
                    if len(aux) > 1:
                        entidad = aux[1]

                if len(row) != 0 and row[0].strip().isdigit():
                    toappend = []
                    toappend.extend([comunitat, provincia, entidad, row[0]])
                    toappend.extend([row[i] for i in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]])
                    llista_final.append(toappend)

            # Add the processed rows to the final CCAA list
            for line in llista_final:
                llista_final_ccaa.append(line)

        # Add header row
        capcelera = [
            'COMUNITAT_AUTONOMA', 'PROVINCIA', 'ENTITAT', 'CODI PROJECTE', 'DENOMINACIO', 
            'COST TOTAL', 'INICI', 'FI', 'TIPUS', 'ANY_ANTERIOR', 'ANY_ACTUAL', 
            'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3'
        ]
        llista_final_ccaa.insert(0, capcelera)

        # Generate upload file path
        upload_file = (
            'Inversions de lestat/' + str(Year_global) + 
            '/Inversio pressupostada/4.Sector Publico Empresarial/' + 
            str(Year_global) + "_PRES_FACT_DET_SP_EMPR.csv"
        )

        # Upload the final data to the blob
        subida_blob(upload_file, llista_final_ccaa)

        return func.HttpResponse("Sector Empresarial carregat", status_code=200)

    except Exception as e:
        # Log and return an error response if something goes wrong
        return func.HttpResponse(f"Error processing empresarial_function: {str(e)}", status_code=500)
