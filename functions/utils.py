import os
import pandas as pd
import azure.storage.blob
from azure.storage.blob import BlobClient
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

# Configuration parameters
CONNECTION_STRING = os.environ['CUSTOMCONNSTR_storage']
CONTAINER_NAME = "contenedorinversionsestat"
connectionString = os.environ['CUSTOMCONNSTR_storage']
containerName = "contenedorinversionsestat"
Arxius_processats=[]

def descarga_blob(download_file):
    """Download a file from Blob storage"""
    blob = BlobClient.from_connection_string(conn_str=CONNECTION_STRING, container_name=CONTAINER_NAME, blob_name=download_file)
    
    # Download the blob
    download_stream = blob.download_blob()
    datos = download_stream.readall()

    # Decode the data (first try cp1252, then fallback to utf-8)
    try:
        str_datos = datos.decode('cp1252')
    except:
        str_datos = datos.decode('utf8')

    # Clean up the data
    pattern = re.compile(r'".*?"', re.DOTALL)
    str_datos = pattern.sub(lambda x: x.group().replace('\n', ' '), str_datos) 

    # Create a list from the rows
    llista_descarregada = [row.split(';') for row in str_datos.splitlines()]
    return llista_descarregada

def subida_blob(upload_file_name, llista_final):
    """Upload a list to Blob storage as a CSV file"""
    blob = BlobClient.from_connection_string(conn_str=CONNECTION_STRING, container_name=CONTAINER_NAME, blob_name=upload_file_name)
    
    # Create DataFrame and upload to Blob storage
    df = pd.DataFrame(llista_final[1:], columns=llista_final[0])
    data = df.to_csv(index=False, sep=";")
    blob.upload_blob(data, overwrite=True)


def individual_SS(llista, llistafinal, provincia, llistaentitat): #PARA SEGURETAT SOCIAL
    imptotal = ''
    anyactual = ''
    any1 = ''
    any2 = ''
    any3 = ''
    entitat = ''
    contador = 1
    for row in llista:
        organisme = row[0] if row[0] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        programa = row[1] if row[1] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        article = row[2] if row[2] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        centretipus = row[3] if row[3] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        numero = row[4] if row[4] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        denominacio = row[5] if row[5] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        inici = row[6] if row[6] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        final = row[7] if row[7] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        imptotal = row[8] if row[8] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        anyanterior = row[9] if row[9] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        anyactual = row[10] if row[10] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        any1 = row[11] if row[11] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        any2 = row[12] if row[12] != '' and 'TOTAL' not in row[5].split(' ')[0] else ''
        if row[5] != '' and 'TOTAL' in row[5].split(' ')[0] and 'ENTIDAD' in row[5].split(' ')[1][:7]:
            toappendentitat = []
            #seleccionamos solamente lo que haya después del numero, para quedarnos solamente con el nombre de cada entidad;
            noment = row[5]
            nomentitat = noment.replace('\n', ' ')
            patron = r'\b\d+\b\s*(.*)'
            coincidencia = re.search(patron, nomentitat)
            if coincidencia:
                nomentitatdef=coincidencia.group(1)
                toappendentitat.extend([provincia[:4] + str(contador), nomentitatdef])
                llistaentitat.append(toappendentitat)
                contador = contador + 1

        if 'TOTAL' not in row[5].split(' ')[0]:
            toappend = []
            toappend.extend([organisme, programa, article, centretipus, numero, denominacio, inici, final,
                             imptotal, anyanterior, anyactual, any1, any2, provincia, provincia[:4] + str(contador)])
            llistafinal.append(toappend)

def individual(llista,llista_final): #para ESTADO
    tot_ministeri = llista[3][1].split(':')[1]
    ministeri = tot_ministeri[4:]
    id_ministeri = tot_ministeri.split(' ')[1]
    Id_CCAA = llista[4][1].split(':')[1]
    CCAA = Id_CCAA.split(' ')
    #Cogemos el nombre de la comunidad entero, tambiém si tiene mas de una palabra
    ccaa_final = ' '.join(CCAA[2:])
    x = 0
    id_org = ''
    desc_org = ''
    idprograma = ''
    article = ''
    for x,row in enumerate(llista):
        if row != [] and (len(llista)-7) > x > 10 and row[4] != 'TOTAL' and (row[11] != '' or row[12] != '' or
            row[13] != '' or row[14] != '' or row[15] != '' or row[10] != ''):
                if row[0] != '':
                    id_org = row[0]
                    desc_org = row[4]
                if row[1] != '':
                    idprograma = row[1]
                if row[2] != '':
                    article = row[2]
                if row[3] != '':
                    toappend = []
                    toappend.extend([id_ministeri, ministeri, ccaa_final, id_org, idprograma, article, desc_org])
                    toappend.extend(list(row[i] for i in [3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15]))
                    llista_final.append(toappend)
    return llista_final