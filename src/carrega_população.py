import pandas as pd
from sqlalchemy import create_engine
from utils import pre_processamento, corrigir_cidade

# Caminho para o CSV
csv_path = "/home/thiago/python-projects/crime-sp/data/raw/auxiliar/serie_populacao2000a2023.csv"

# Conexão com o banco
engine = create_engine("postgresql://thiago:1234@localhost:5432/crimesp")

# Leitura
df = pd.read_csv(csv_path, encoding= 'ISO-8859-1', sep=';')

# Renomeia as colunas para maiúsculo
df.columns = [col.upper() for col in df.columns]

# Pré-processa e corrige nomes de município
df['MUNICIPIO'] = df['MUNICIPIO'].apply(pre_processamento)
df['MUNICIPIO'] = df['MUNICIPIO'].apply(corrigir_cidade)

# Sobe para o banco
df.to_sql("populacao_municipios", con=engine, if_exists='replace', index=False)

print("População tratada, normalizada e carregada com sucesso.")
