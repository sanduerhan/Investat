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


# Parametros de conexion Blob Storage
connectionString = os.environ['CUSTOMCONNSTR_storage']
containerName = "contenedorinversionsestat"
Arxius_processats=[]


# Parametros de conexión a SharePoint
baseurl = 'https://gencat.sharepoint.com'
basesite =  os.environ['CUSTOMCONNSTR_basesite'] #'/sites/ProvespythonInvEstat' 
siteurl = baseurl + basesite
relative_file_path = basesite + '/Documents compartits/'
relative_file_path_no_slash =  relative_file_path[:-1]
username = os.environ['CUSTOMCONNSTR_username'] #config.username
pwd = os.environ['CUSTOMCONNSTR_password'] #config.password




def descarga_lista_sharepoint(lista_ficheros, Arxius_processats):
    ctx_auth = AuthenticationContext(siteurl)
    ctx_auth.acquire_token_for_user(username, pwd)
    ctx = ClientContext(siteurl, ctx_auth)

    for carpeta in lista_ficheros:
        # Obtener todos los archivos de la carpeta en SharePoint
        folder_url = relative_file_path + carpeta
        folder = ctx.web.get_folder_by_server_relative_path(folder_url)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        for file in files:
            # Bajada y subida de cada archivo
            file_1=file.properties['Name']
            down_file_path = relative_file_path + carpeta + '/' + file_1
            Arxius_processats.append(carpeta + '/' +file_1)
            print(Arxius_processats)
            blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=carpeta+'/'+file_1)

            response = File.open_binary(ctx, down_file_path)
            blob.upload_blob(response.content, overwrite=True)

def descarga_blob(download_file):
   # Nos conectamos al Blob Storage
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=download_file)
    
   # Descargamos el fichero y guardamos su valor en una variable
   download_stream = blob.download_blob()
   datos = download_stream.readall()
   
   # Decodificamos los datos ANSI (cp1252)
   try:
      str_datos = datos.decode('cp1252')
    #Decodificamos los datos UTF8 si el otro encoding falla
   except:
      str_datos = datos.decode('utf8')
   
   # Eliminamos salos de linea que pueda haber entre comillas
   pattern = re.compile(r'".*?"', re.DOTALL)
   str_datos = pattern.sub(lambda x: x.group().replace('\n', ' '), str_datos) 
    
   # A partir de los datos generamos una lista
   llista_descarregada = []
   for row in iter(str_datos.splitlines()):
       llista_descarregada.append(row.split(';'))
       
   return llista_descarregada

def subida_blob(upload_file_name,llista_final):
   # Creamos una conexión con un nuevo nombre de destino
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=upload_file_name)  
   print('subida_blob')
   print(llista_final[0])
   print('datos')
   print(llista_final[1:])
   # Cargamos los datos a un dataframe
   df = pd.DataFrame(llista_final[1:],columns = llista_final[0])
   data = df.to_csv(index=False,sep=";")
   # Los subimos a Blob Storage
   blob.upload_blob(data,overwrite=True)
   
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

app = Flask(__name__)

# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
    print(Arxius_processats)
    return "l''App está activa"

# Movemos los ficheros del SharePoint al Blob Storage
@app.route('/download/<int:Year_global>', methods=['GET'])
def download_files(Year_global: int):
    Arxius_processats.clear()
    # Uso de la función
    lista_ficheros = ['Inversions de lestat/'+str(Year_global)+'/Execucio pressupostada',
                      'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/1.Estado',
                      'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/2.Seguretat Social',
                      'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/3.Sector Publico Administrativo',
                      'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/4.Sector Publico Empresarial',
                      'Inversions de lestat/'+str(Year_global)+'/Licitacions i adjudicacions']
    
    descarga_lista_sharepoint(lista_ficheros, Arxius_processats)

    return str(Arxius_processats)
#ESTADO
@app.route('/Estado/<int:Year_global>', methods=['GET'])
def estado(Year_global: int):
    # Nos quedamos con los últimos digitos del año
    year_YY = Year_global - 2000
    llista_final = []
    # Descargamos los ficheros que empiezan que están en str(Year_global)+'/Inversio pressupostada/1.Estado
    for nom_arxiu in Arxius_processats:
        if str(Year_global)+'/Inversio pressupostada/1.Estado' in nom_arxiu:
            fitxer_estado = descarga_blob(nom_arxiu)
            llista_final= individual(fitxer_estado,llista_final)
            print(nom_arxiu)
    capcelera = ['ID_MINISTERI','DESC_MINISTERI' ,'COMUNITAT_AUTONOMA', 'CODI_CENTRE','ID_PROGRAMA', 'ID_ARTICLE',
             'DESC_CENTRE','ID_PROJECTE', 'NOM_PROJECTE',
             'ANY_INICI', 'ANY_FI', 'PROVINCIA', 'TIPUS', 'COST_TOTAL', 'ANY_ANTERIOR', 'ANY_ACTUAL',
             'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']

    llista_final.insert(0, capcelera)
    upload_file	= 'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/1.Estado/'+ str(Year_global)+ "_PRES_FACT_DET_EST_OOAA_RE.csv"
    print(upload_file)
    print(llista_final)
    subida_blob(upload_file,llista_final)
    return "Fitxer d'estado carregats"
#SEGURETAT SOCIAL INVERSIO PRESSUPOSTADA
@app.route('/SS/<int:Year_global>', methods=['GET'])
def SS_script(Year_global: int):
    ruta_arxius= 'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/2.Seguretat Social/'
    llistafinal = []
    llistaentitat = []

    # Nos quedamos con los últimos digitos del año
    year_YY = Year_global - 2000

    header = ['Organisme', 'Programa', 'Article', 'Centre Tipus', 'Número', 'Denominació', 'Inici', 'Final',
            'Import Total', 'ANY-1', 'ANY_EXC_PRESSUPOSTARI', 'ANY+1', 'ANY+2', 'Província', 'ID Entitat']
    headerentitat = ['ID Entitat', 'Entitat']

    llistaentitat.insert(0, headerentitat)
    llistafinal.insert(0, header)

    for nom_arxiu in Arxius_processats:
        if str(Year_global)+'/Inversio pressupostada/2.Seguretat Social' in nom_arxiu:
            provincia = nom_arxiu.split('/')[-1][6:-4]
            llista_provincia = descarga_blob(nom_arxiu)
            individual_SS(llista_provincia, llistafinal, provincia, llistaentitat)
            
    upload_file	= ruta_arxius + str(Year_global) + "_PRES_FACT_DET_SEGURETAT_SOCIAL.csv"
    subida_blob(upload_file,llistafinal)

    upload_file	= ruta_arxius + str(Year_global) +"_DIM_DET_SEGURETAT_SOCIAL_ENTITATS.csv"
    subida_blob(upload_file,llistaentitat)    

    return 'Blob SS subido'
#SECTOR ADMINISTRATIVO INVERSIO PRESSUPOSTADA
@app.route('/Sector_Administratiu/<int:Year_global>', methods=['GET'])
def administratiu(Year_global: int):
    llista_final_ccaa =[]
    for nom_arxiu in Arxius_processats:
        if str(Year_global)+'/Inversio pressupostada/3.Sector Publico Administrativo' in nom_arxiu:
            llista_origen = descarga_blob(nom_arxiu)
            llista_final = []
            comunitat = ""
            provincia = ""
            entidad = ""
            for row in llista_origen:

                if len(row) != 0 and "PROVINCIA" in row[0]:
                    posiciones_dos_puntos = [i for i, char in enumerate(row[0]) if char == ":"]
                    posicion_pro = [i for i in range(len(row[0])) if row[0].startswith("PRO", i)]
                    provincia = row[0][posiciones_dos_puntos[1]+2:]
                    comunitat = row[0][posiciones_dos_puntos[0]+2:posicion_pro[0]]

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
                    toappend.extend(list(row[i] for i in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
                    llista_final.append(toappend)
            for line in llista_final:
                llista_final_ccaa.append(line)    

    capcelera = ['COMUNITAT_AUTONOMA', 'PROVINCIA', 'ENTITAT', 'CODI PROJECTE', 'DENOMINACIO', 'COST TOTAL', 'INICI', 'FI','TIPUS', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
    llista_final_ccaa.insert(0, capcelera)
    upload_file	= 'Inversions de lestat/'+str(Year_global)+'/Inversio pressupostada/3.Sector Publico Administrativo/' + str(Year_global) + "_PRES_FACT_DET_SP_ADMIN.csv"
    subida_blob(upload_file,llista_final_ccaa)
    return 'Sector Administratiu carregat'
#SECTOR EMPRESARIAL INVERSIO PRESSUPOSTADA
@app.route('/Sector_Empresarial/<int:Year_global>', methods=['GET'])
def empresarial(Year_global: int):
    llista_final_ccaa =[]
    for nom_arxiu in Arxius_processats:
        if str(Year_global)+'/Inversio pressupostada/4.Sector Publico Empresarial' in nom_arxiu:
            llista_origen = descarga_blob(nom_arxiu)
            llista_final = []
            comunitat = ""
            provincia = ""
            entidad = ""
            for row in llista_origen:

                if len(row) != 0 and "PROVINCIA" in row[0]:
                    posiciones_dos_puntos = [i for i, char in enumerate(row[0]) if char == ":"]
                    posicion_pro = [i for i in range(len(row[0])) if row[0].startswith("PRO", i)]
                    provincia = row[0][posiciones_dos_puntos[1]+2:]
                    comunitat = row[0][posiciones_dos_puntos[0]+2:posicion_pro[0]]

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
                    toappend.extend(list(row[i] for i in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
                    llista_final.append(toappend)
            for line in llista_final:
                llista_final_ccaa.append(line)  

    capcelera = ['COMUNITAT_AUTONOMA', 'PROVINCIA', 'ENTITAT', 'CODI PROJECTE', 'DENOMINACIO', 'COST TOTAL', 'INICI', 'FI','TIPUS', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
    llista_final_ccaa.insert(0, capcelera)

    upload_file	= 'Inversions de lestat/'+str(Year_global) + '/Inversio pressupostada/4.Sector Publico Empresarial/'+ str(Year_global) + "_PRES_FACT_DET_SP_EMPR.csv"
    subida_blob(upload_file,llista_final_ccaa)
    return 'Sector Empresarial carregat'























# Iniciamos nuestra app
"""if __name__ == '__main__':
   app.run()"""