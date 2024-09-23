import argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup
import warnings
import psycopg2
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
warnings.filterwarnings('ignore')

print("-------------EMPEZO A CORRER EL SCRIPT------------")

load_dotenv()

def create_db_connection(uri):
    engine = None
    try:
        engine = create_engine(uri)
        with engine.connect():
            print(f"CONNECTED TO {engine.url}")
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

BASE_URL = "https://datos.gob.ar/dataset/mincyt-proyectos-ciencia-tecnologia-e-innovacion"
BASE_DIR = 'C:\\Users\\PC\\Documents\\Matias\\data_projects\datos_abiertos_ciencia_tecnologia'

database_url = os.getenv("DATABASE_URL")

def extract_links(url):
  r = requests.get(url)
  if r.status_code != 200:
    print('url is invalid, cannot access')
  soup = BeautifulSoup(r.text, "html.parser")
  links_csvs = [link['href'] for link in soup.find_all('a') if 'csv' in link.get('href', '') and 'proyectos_20' in link.get('href', '')]
  return links_csvs

def extract_data(links, anios=[]):
  if len(anios) > 0:
    for link in links:
      if any(str(anio) in link for anio in anios):
        df = pd.read_csv(link, sep=None)
        anio_ = link.split("/")[8][10:14]
        df['año'] = anio_
        df.to_csv(f'{BASE_DIR}/CSV/proyectos_{anio_}.csv', index=False)
        print(f"File '{BASE_DIR}/CSV/proyectos_{anio_}.csv' was saved successfully")
  else:
    for link in links:
      df = pd.read_csv(link, sep=None)
      anio_ = link.split("/")[8][10:14]
      df['año'] = anio_
      df.to_csv(f'{BASE_DIR}/CSV/proyectos_{anio_}.csv', index=False)

def transform_data(anios=[]):
    proyectos = pd.DataFrame()
    files = os.listdir(BASE_DIR + '/CSV')
    if len(anios) > 0:
        for file in files:
            if any(str(anio) in file for anio in anios):
                df = pd.read_csv(BASE_DIR+'/CSV/'+file)
                proyectos = pd.concat([proyectos, df], ignore_index=True)
                proyectos['fecha_inicio'] = pd.to_datetime(proyectos['fecha_inicio'], errors='coerce')
                proyectos['fecha_finalizacion'] = pd.to_datetime(proyectos['fecha_finalizacion'], errors='coerce')
                proyectos['duracion'] = proyectos['fecha_finalizacion']-proyectos['fecha_inicio']
                proyectos['duracion'] = proyectos['duracion'].dt.days
    else:
        for file in files:
            df = pd.read_csv(BASE_DIR+'/CSV/'+file)
            proyectos = pd.concat([proyectos, df], ignore_index=True)
            proyectos['fecha_inicio'] = pd.to_datetime(proyectos['fecha_inicio'], errors='coerce')
            proyectos['fecha_finalizacion'] = pd.to_datetime(proyectos['fecha_finalizacion'], errors='coerce')
            proyectos['duracion'] = proyectos['fecha_finalizacion']-proyectos['fecha_inicio']
            proyectos['duracion'] = proyectos['duracion'].dt.days
    return proyectos

def load_data(output):
    engine = create_db_connection(database_url)
    if engine:
        output.to_sql('science_projects', engine, if_exists='replace', schema='public', index=False)
        print("TABLE science_projects SAVED SUCCESSFULLY")
    else:
        print("Failed to connect to database.")

def main():
    parser = argparse.ArgumentParser(description="Procesa datos de proyectos científicos.")
    parser.add_argument('--anios', nargs='+', type=int, help="Lista de años a procesar")
    args = parser.parse_args()

    anios = args.anios if args.anios else []
    print(f"Procesando los años: {anios}")
    print("------------------------------EXTRACT LINKS----------------------------------")
    links = extract_links(BASE_URL)
    print("------------------------------EXTRACT DATA----------------------------------")
    extract_data(links, anios)
    print("------------------------------TRANSFORM DATA----------------------------------")
    proyectos = transform_data()
    print("------------------------------LOAD DATA----------------------------------")
    load_data(proyectos)

if __name__ == "__main__":
    main()