#!/usr/bin/env python
# coding: utf-8

## Instalar las librerias necesarias

# pip install pandas
# pip install numpy

# pip install sqlalchemy
# pip install mssql
# pip install pyodbc
# pip install pymssql

# pip install matplotlib
# pip install BeautifulSoup4
# pip install tabula-py
# pip install xlrd
# pip install lxml
# pip install openpyxl

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import requests
from tabula import read_pdf
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import os
import math
import logging
import datetime
from tipoCambioSII import valorDolarSII
import sys

directorio_logs = "logs"
if not os.path.exists(directorio_logs):
    os.makedirs(directorio_logs)

datetime_actual = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
nombre_archivo_log = f"{directorio_logs}/etl_supertienda_{datetime_actual}.log"

logging.basicConfig(
    level = logging.INFO,
    format = '[%(levelname)s] - %(asctime)s - %(message)s',
    filename = nombre_archivo_log,
    filemode = 'w'
)

##### INDICAR EL INICIO DEL PROGRAMA ######
logging.info("COMIENZA EJECUCION DEL ETL DE SUPERTIENDA")


### Abrir una conexion con una base de datos SQL Server
#Conector: mssql+pyodbc
server = 'mssql-171691-0.cloudclusters.net'
port = '15677'
database = 'SuperTienda'
username = 'etl_svcaccount'
password = 'D3v.2024'

engine = create_engine( f"mssql+pymssql://{username}:{password}@{server}:{port}/{database}" )

try:
    logging.info("Intentando Establecer conexion con la base de datos de la SuperTienda...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT '1'"))
        logging.info("CONEXION EXITOSA!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)

try:
    logging.info("Cargando Query_01...")
    df_query1 = pd.read_sql("""SELECT *
                                FROM [Pedidos Detalles] AS pd
                                INNER JOIN [Pedidos] AS p
                                ON p.[ID Pedido] = pd.[ID Pedido]
                                INNER JOIN Clientes AS c
                                ON c.[ID Cliente] = p.[Cliente ID]
                                INNER JOIN Segmentos AS s
                                ON s.[ID Segmento] = c.[ID Segmento]
                              """, engine )
    logging.info("Done!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)

try:
    logging.info("Consultando Query_02...")
    df_query2 = pd.read_sql("SELECT * FROM [Sub-Categorias] AS scat INNER JOIN Categorias AS cat ON cat.[ID Categoria] = scat.[ID Categoria]", engine)
    logging.info("Consulta Query_02 Exitosa!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)

try:
    logging.info("Abriendo archivo de Productos...")
    df_productos = pd.read_csv(".//Datos//Productos.csv", delimiter='\t', encoding='ansi')
    
    logging.info("Procesando archivo de Productos...")
    df_productos.columns = ["ID Artículo","ID Sub-Categoria","Producto","PrecioUnitario","CostoUnitario"]

    df_productos["PrecioUnitario"] = df_productos["PrecioUnitario"].str.replace("$", "")
    df_productos["CostoUnitario"] = df_productos["CostoUnitario"].str.replace("$", "")

    df_productos["PrecioUnitario"] = df_productos["PrecioUnitario"].str.replace(".", "")
    df_productos["CostoUnitario"] = df_productos["CostoUnitario"].str.replace(".", "")
    
    df_productos["PrecioUnitario"] = df_productos["PrecioUnitario"].str.replace(",", ".")
    df_productos["CostoUnitario"] = df_productos["CostoUnitario"].str.replace(",", ".")

    df_productos["PrecioUnitario"] = pd.to_numeric(df_productos["PrecioUnitario"])
    df_productos["CostoUnitario"] = pd.to_numeric(df_productos["CostoUnitario"])
    
    logging.info("Procesaminto Exitoso!...")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)

try:
    logging.info("Abriendo Excel de Ciudades...")
    df_ciudades = pd.read_excel(".//Datos/Ciudades.xlsx", header=0, sheet_name="Hoja1")
    logging.info("Exito!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)



#cargamos los datos auxiliares. En primer lugar, la lista de paises proveniente de un PDF.
try:
    logging.info("Abriendo PDF de Paises...")
    pdf_paises = ".//Datos//Paises.pdf"

    df_paises_aux = read_pdf(
                       pdf_paises, 
                       pages = [1,2,3,4],
                       multiple_tables = True, 
                       #pandas_options={'skiprows':1},
                       pandas_options={'header':None},
                       encoding = 'ISO-8859-1',
                       stream = False
                       )

    logging.info("Procesando PDF de Paises...")
    df_paises = pd.concat(df_paises_aux)

    #Promovemos la primera fila como encabezado
    df_paises.columns = df_paises.iloc[0]
    df_paises = df_paises[1:]
    logging.info("Exito!")

except Exception as ex:
    logging.error(ex)
    sys.exit(1)


## OBTENER LOS VALORES DEL DOLAR DE LOS AÑOS 2011 a 2014

try:
    logging.info("Leyendo paginas del SII con los tipos de Cambio...")
    df_tipoCambio2011 = valorDolarSII("https://www.sii.cl/pagina/valores/dolar/dolar2011.htm", "468,37", 2011)
    df_tipoCambio2012 = valorDolarSII("https://www.sii.cl/pagina/valores/dolar/dolar2012.htm", "521,46", 2012)
    df_tipoCambio2013 = valorDolarSII("https://www.sii.cl/valores_y_fechas/dolar/dolar2013.htm", "478,60", 2013)
    df_tipoCambio2014 = valorDolarSII("https://www.sii.cl/valores_y_fechas/dolar/dolar2014.htm", "523,76", 2014)
    df_tipoCambio2011a2014 = pd.concat([df_tipoCambio2011, df_tipoCambio2012, df_tipoCambio2013,df_tipoCambio2014])
    logging.info("Exito!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)


try:
    logging.info("Comienza Consolidación de datos...")
    logging.info("Query 01 con Prodcutos...")
    df_supertienda = pd.merge(df_query1, df_productos, how="left", left_on="Articulo ID", right_on="ID Artículo")
    logging.info("Consolidadndo con Query_02...")
    df_supertienda = pd.merge(df_supertienda, df_query2, how="left", left_on="ID Sub-Categoria", right_on="ID Sub-Categoria")
    logging.info("Consolidadndo con Ciudades...")
    df_supertienda = pd.merge(df_supertienda, df_ciudades, how="left", left_on="ID Ciudad", right_on="ID Ciudad")
    logging.info("Consolidadndo con Paises...")
    df_supertienda = pd.merge(df_supertienda, df_paises, how="left", left_on="ID Pais", right_on="ID Pais")
    logging.info("Consolidadndo con Valor Tipo de Cambio...")
    df_supertienda = pd.merge(df_supertienda, df_tipoCambio2011a2014, how="left", left_on="Fecha Pedido", right_on="Fecha")
    logging.info("Eliminando columnas repetidas...")
    df_supertienda = df_supertienda.loc[:,~df_supertienda.T.duplicated(keep='first')]
    logging.info("Exito!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)




try:
    logging.info("Calculando metricas adicionales...")
    df_supertienda["Venta_USD"] = df_supertienda["PrecioUnitario"] * df_supertienda["Cantidad"] * (1-df_supertienda["Descuento"])
    df_supertienda["Costo_USD"] = ( df_supertienda["CostoUnitario"] * df_supertienda["Cantidad"] ) + df_supertienda["Coste envío"]
    df_supertienda["Utilidad_Bruta_USD"] = df_supertienda["Venta_USD"] - df_supertienda["Costo_USD"]

    df_supertienda["IVA"] = df_supertienda["Venta_USD"] * (0.19/1.19)
    df_supertienda["Factor_PuntosSuperTienda"] = df_supertienda["Venta_USD"] * df_supertienda["Factor_PuntosSuperTienda"]

    df_supertienda["Utilidad_Neta_USD"] = df_supertienda["Utilidad_Bruta_USD"] - df_supertienda["IVA"] - df_supertienda["Factor_PuntosSuperTienda"]

    df_supertienda = df_supertienda.round({"Venta_USD":2, "Costo_USD":2, "Utilidad_Bruta_USD":2, "IVA":2, "Factor_PuntosSuperTienda":2, "Utilidad_Neta_USD":2})
    logging.info("Exito!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)




### Abrir una nueva conexion con una base de datos SQL Server
### PARA ALMACENAR DATOS DEL PIPELINE
#Conector: mssql+pyodbc
server_dw = 'mssql-171691-0.cloudclusters.net'
port_dw = '15677'
database_dw = 'SuperTienda'
username_dw = 'etl_svcaccount'
password_dw = 'D3v.2024'

engine_dw = create_engine( f"mssql+pymssql://{username_dw}:{password_dw}@{server_dw}:{port_dw}/{database_dw}" )



try:
    logging.info("Intentando Establecer conexion con el Data Warehouse de la SuperTienda...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT '1'"))
        logging.info("CONEXION EXITOSA!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)


import math
df_num_of_cols = len(df_supertienda.columns)
chunknum = math.floor(2100/df_num_of_cols)

try:
    logging.info("Subiendo Datos al DW...")
    df_supertienda.to_sql(
        "ST_NOMBRE_APELLIDO",
        engine_dw,
        if_exists="append",
        method='multi',
        index=False,
        chunksize=chunknum)
    logging.info("EXITO!")
except Exception as ex:
    logging.error(ex)
    sys.exit(1)

engine.dispose()
engine_dw.dispose()
logging.info("##### FINALIZA ETL SUPERTIENDA ######")



