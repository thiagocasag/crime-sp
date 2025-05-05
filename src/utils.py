import os
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
import textdistance as txd
import geopandas as gpd
import folium
from folium.plugins import HeatMap



def pre_processamento(palavra):

    nova_palavra = palavra
    if isinstance(palavra, str):

        nova_palavra = nova_palavra.replace('ç', 'c')
        nova_palavra = nova_palavra.replace('ú', 'u')
        nova_palavra = nova_palavra.replace('í', 'i')
        nova_palavra = nova_palavra.replace('â', 'a')
        nova_palavra = nova_palavra.replace('ã', 'a')
        nova_palavra = nova_palavra.replace('á', 'a')
        nova_palavra = nova_palavra.replace('é', 'e')
        nova_palavra = nova_palavra.replace('ê', 'e')
        nova_palavra = nova_palavra.strip().upper()

    return nova_palavra



def palavra_mais_proxima(cidade, ibge_data):

    # cidade = 'SEVERÍNIA'
    lista_ibge = ibge_data['municipio'].unique()
    cidade = cidade.strip()
    equal_word = 'palavraabsolutamentealeatoriataoaleatoriaqueadistanciaehenorme'
    distancia = txd.levenshtein(cidade, equal_word)

    for palavra in lista_ibge:
        if type(palavra) == str:
            nova_distancia = txd.levenshtein(cidade, palavra)
            if nova_distancia < distancia:
                distancia = nova_distancia
                equal_word = palavra

    return equal_word



def corrigir_cidade(nome_cidade):
    if isinstance(nome_cidade, str):
        nome_cidade = nome_cidade.strip()
        if nome_cidade == 'S.PAULO':
            return 'SAO PAULO'
        elif nome_cidade == 'S.BERNARDO DO CAMPO':
            return 'SAO BERNARDO DO CAMPO'
        elif nome_cidade == 'S.ANDRE':
            return 'SANTO ANDRE'
        elif nome_cidade == 'S.CAETANO DO SUL':
            return 'SAO CAETANO DO SUL'
        elif nome_cidade == 'S.ISABEL':
            return 'SANTA ISABEL'
        elif nome_cidade == 'S.LOURENCO DA SERRA':
            return 'SAO LOURENCO DA SERRA'
        elif nome_cidade == 'S.VICENTE':
            return 'SAO VICENTE'
        elif nome_cidade == 'S.ROQUE':
            return 'SAO ROQUE'
    return nome_cidade



def correcao_horario(hora):

    string_hora = str(hora)
    n_hora = string_hora[0:2] + ':' + string_hora[-2:] 

    return n_hora



def categorizacao_horario(horario):
    if pd.isna(horario):
        return np.nan
    elif 0 <= horario.hour < 6:
        return 'Madrugada'
    elif 6 <= horario.hour < 12:
        return 'Manhã'
    elif 12 <= horario.hour     < 18:
        return 'Tarde'
    else:
        return 'Noite'


def dropa_coluna_vazia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas cujo nome é vazio, só espaços ou do tipo 'Unnamed: 0', 'Unnamed: 1', etc.
    """
    empty_name = df.columns.str.strip() == ""                 # nomes vazios
    unnamed    = df.columns.str.match(r"^Unnamed:\s*\d+$")    # Unnamed: N
    mask = empty_name | unnamed
    return df.loc[:, ~mask]      # mantém apenas as que NÃO estão no mask




def tratamento(df: pd.DataFrame) -> pd.DataFrame:
    # Cria uma cópia do DataFrame para evitar o warning "SettingWithCopy"
    df = df.copy()

    # Converte todas as colunas do tipo object para string explicitamente.
    # Isso evita erros ao salvar como Parquet, onde o pyarrow exige consistência de tipos.
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    # Remove colunas vazias ou irrelevantes como 'Unnamed: 0', espaços em branco, etc.
    df = dropa_coluna_vazia(df)

    # Aplica pré-processamento de texto nas colunas do tipo string (uppercase, acento, etc.)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(pre_processamento)

    # Corrige nomes específicos de cidades, se a coluna 'CIDADE' estiver presente
    if 'CIDADE' in df.columns:
        df['CIDADE'] = df['CIDADE'].apply(corrigir_cidade)

    # Se existir a coluna 'HORARIO', corrige o formato e categoriza o período do dia
    col_hora = next((col for col in ['HORARIO'] if col in df.columns), None)
    if col_hora:
        # Corrige o horário no formato HH:MM
        df['HORA_FORMATADA'] = df[col_hora].apply(correcao_horario)

        # Converte para datetime, com tratamento de erros
        df['HORA_FORMATADA'] = pd.to_datetime(df['HORA_FORMATADA'], format='%H:%M', errors='coerce')

        # Cria uma nova coluna categorizando o horário como Manhã, Tarde, etc.
        df['PERIODO'] = df['HORA_FORMATADA'].apply(categorizacao_horario)

    # Retorna o DataFrame tratado
    return df


# def importacao(path):
#     #IMPORTA TODOS OS DATAFRAMES DE BO'S:

#     df_dicio = {}

#     directory = os.listdir(path)

#     for file in directory:
#         if file != 'auxiliar':
#             if file.startswith('RDO'):
#                 nome_filev = file
#                 df_v = pd.read_csv(path + '/' + nome_filev)
#                 nome_key = file[:-4]
#                 df_dicio[nome_key] = df_v


#             elif file[:-4][-1] == '1':        #para os files que representam 1o semestres:
#                 for file2 in directory:
#                     if (file != file2) and (file[:-5] == file2[:-5]):       #encontrando o 2o semestre do ano em questao:
#                         nome_file1 = file
#                         nome_file2 = file2
#                         df1 = pd.read_csv(path + '/' + nome_file1)
#                         df2 = pd.read_csv(path + '/' + nome_file2)

#                         nome_key = file[:-6]

#                         df_dicio[nome_key] = pd.concat([df1 ,df2], ignore_index=True)   #juntando os dois semestres.

#             elif (file[:-4][-1] != '1') and (file[:-4][-1] != '2'):       #para os files que nao sao subdivididos em 2 semestres:
#                 nome_file = file
#                 df = pd.read_csv(path + '/' + nome_file)
#                 nome_key = file[:-4]
#                 df_dicio[nome_key] = df

#     return df_dicio
