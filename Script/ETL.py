import pyodbc
import logging
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf


#directorios para guardar los logs

log_dir = './logs'
data_dir = './data'

#si no existe crea los directorios log

if not os.path.exists(log_dir):
	os.makedirs(log_dir)
if not os.path.exists(data_dir):
	os.makedirs(data_dir)

#Configuración del logging

log_filename = os.path.join(log_dir, 'Etapa3.log')

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	handlers=[
		logging.FileHandler(log_filename),
		logging.StreamHandler()
	]
)

# Configuración de la conexión
server = 'DESKTOP-UOCC7RM\SQLEXPRESS'
database = 'Nivelacion'
username = 'sa'
password = '808118'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'



#Obtener listado S&P500

def extract_data():
	try:
		logging.info(f'Extrayendo listado S&P500')
		url = "https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500"
		data = pd.read_html(url)
		data = data[0]
		logging.info(f'listado S&P500 extraído exitosamente')
		return data
	except Exception as e:
		logging.error(f'Error extrayendo listado S&P500 de Wikipedia: {e}')
		return None

def transform_data(data):
	try:
		logging.info('Transformando datos')
		dfC = data[['Símbolo', 'Seguridad', 'Sector GICS', 'Ubicación de la sede', 'Fundada']]
		dfC.rename(columns={'Seguridad': 'Nombre'}, inplace=True)
		logging.info('Datos transformados exitosamente')
		return dfC
	except Exception as e:
		logging.error(f'Error transformando datos: {e}')
		return None

def load_data(dfC):
    try:
        filename = os.path.join(data_dir, f'S&P500_list.csv')
        logging.info(f'Guardando datos transformados en {filename}')
        dfC.to_csv(filename, index=False)
        logging.info('Datos guardados exitosamente')
    except Exception as e:
        logging.error(f'Error guardando datos: {e}')

def etl_process():
    data = extract_data()
    if data is not None:
      Transformed_data = transform_data(data)
      if Transformed_data is not None:
        load_data(Transformed_data)
        return Transformed_data
    return None

data = etl_process()


#Obtener precios S&P500

def extract_data(data, start_date, end_date):
	tickers = data["Símbolo"]
	df_list = []
	for ticker in tickers:
		try:
			logging.info(f'Extrayendo precios {ticker}')
			price = yf.download(ticker, group_by="Ticker", start=start_date, end=end_date)
			price['Ticker'] = ticker  
			df_list.append(price)
			logging.info(f'precio extraído exitosamente')
		except Exception as e:
			logging.error(f'Error extrayendo precio para {ticker}: {e}')
			return None			
	data = pd.concat(df_list)
	return data

def transform_data(data):
	try:
		logging.info('Transformando datos')
		df = data[['Close', 'Ticker']].reset_index()
		df['Close'] = df['Close'].astype('float64')
		logging.info('Datos transformados exitosamente')
		return df
	except Exception as e:
		logging.error(f'Error transformando datos: {e}')
		return None

def load_data(df):
    try:
        filename = os.path.join(data_dir, f'S&P500_prices.csv')
        logging.info(f'Guardando datos transformados en {filename}')
        df.to_csv(filename, index=False)
        logging.info('Datos guardados exitosamente')
    except Exception as e:
        logging.error(f'Error guardando datos: {e}')

def etl_process(data, start_date, end_date):
    data = extract_data(data, start_date, end_date)
    if data is not None:
      transformed_data = transform_data(data)
      if transformed_data is not None:
        load_data(transformed_data)
        return transformed_data
    return None



# Conexión a la base de datos
def ConexionBD():
    try:
        conn = pyodbc.connect(connection_string)
        logging.info('Conexion a la BD de datos fue exitosa')
        return conn
    except Exception as e:
        logging.error(f'Error generando conexion a la BD: {e}')
        return None

# Crear una tabla (si no existe)
def CrearTablaBD(tabla,query):
    conn = None
    cursor = None
    try:
        conn = ConexionBD()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        # Cerrar la conexión
        #cursor.close()
        #conn.close()
        logging.info(f'Creada la tabla con exito: {tabla}')
    except Exception as e:
        logging.error(f'Error creando la tabla {tabla}: {e}')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Llave primaria
def LlavePrimaria(tabla, pk):
    conn = None
    cursor = None
    pk_columns = pk if isinstance(pk, str) else ", ".join(pk)
    try:
        conn = ConexionBD()
        cursor = conn.cursor()
        constraint = 'PK_'+ tabla
        query = f'''ALTER TABLE {tabla}
                ADD CONSTRAINT {constraint} PRIMARY KEY ({pk_columns})
                '''
        cursor.execute(query)
        conn.commit()
        logging.info(f'Llave primaria creada con exito en {tabla}')
    except Exception as e:
        logging.error(f'Error agregando PK a la {tabla}: {e}')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def LlaveForanea(tablaInicial, tablaRelacion, PKI, PKR):
    conn = None
    cursor = None
    try:
        conn = ConexionBD()
        cursor = conn.cursor()
        constraint = 'FK_'+ tablaInicial + '_' + tablaRelacion
        query = f'''ALTER TABLE {tablaInicial}
                ADD CONSTRAINT {constraint} FOREIGN KEY ({PKI})
                REFERENCES {tablaRelacion}({PKR})
                '''
        cursor.execute(query)
        conn.commit()
        logging.info(f'Llave foranea creada con exito en {tablaInicial}')
    except Exception as e:
        logging.error(f'Error agregando FK a la {tablaInicial}: {e}')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def InsertarDatosBD(registro, query): 
    conn = None
    cursor = None
    try:
        conn = ConexionBD()
        cursor = conn.cursor()
        for i in range(len(registro)):
            cursor.execute(query, registro.iloc[i].to_numpy().tolist())
        conn.commit()
        logging.info(f'Registros insertados en la tabla')
    except Exception as e:
        logging.error(f'Error insertando los datos: {e}')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == '__main__':
    
    
    start_date = '2024-01-01'
    end_date = '2024-03-31'
    data = etl_process(data, start_date, end_date)
 
    
    
    # Creacion de las tablas y llaves primarias
    tabla = "Companies"
    pk = "Simbolo"
    Companies = f'''CREATE TABLE {tabla} ( 
                Simbolo VARCHAR(200) NOT NULL, 
                Nombre VARCHAR(200),
		Sector VARCHAR(200),
		Ubicacion VARCHAR(200),
		Fundada VARCHAR(200));
            ''' 
    CrearTablaBD(tabla,Companies)
    LlavePrimaria(tabla, pk)

    tabla = "CompanyProfiles"
    pk = ("Fecha", "Empresa")
    CP = f'''CREATE TABLE {tabla} ( 
                    Fecha DATE NOT NULL, 
                    PrecioCierre FLOAT, 
                    Empresa  VARCHAR(200) NOT NULL);
                '''
    CrearTablaBD(tabla, CP)
    LlavePrimaria(tabla, pk)

    # Creacion de las llaves foraneas
    tablaInicial = "CompanyProfiles"
    tablaRelacion = "Companies"
    PKI = "Empresa"
    PKR = "Simbolo"
    LlaveForanea(tablaInicial,tablaRelacion,PKI,PKR)

     
    # Datos a insertar   
    dfCom = pd.read_csv('data/S&P500_list.csv')
    query = 'INSERT INTO Companies (Simbolo, Nombre, Sector, Ubicacion, Fundada) VALUES (?, ?, ?, ?, ?)'
    InsertarDatosBD(dfCom, query)

    dfPrices = pd.read_csv('data/S&P500_prices.csv')
    queryP = "INSERT INTO Companies (Fecha, PrecioCierre, Empresa) VALUES (?, ?, ?)"
    InsertarDatosBD(dfPrices, queryP)

 


