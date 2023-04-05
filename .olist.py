import pandas as pd
import os
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from decouple import config

USER = config('USER')
PASSWORD = config('PASSWORD')
HOST = config('HOST')
PORT = config('PORT')
DATABASE = config('DATABASE')

#string_conexao = 'postgresql://postgres:1980@localhost:5432/olist'
string_conexao = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
conexao = create_engine(string_conexao)

if not database_exists(conexao.url):
    create_database(conexao.url)

files = os.listdir('dados')
for file in tqdm(files):
    if 'olist' in file:
        df     = pd.read_csv(f'dados/{file}')
        tabela = file.replace('olist_','').replace('_dataset.csv', '')
        df.to_sql(tabela, conexao, if_exists='replace', index=False)