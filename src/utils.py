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



def importacao(path):
    #IMPORTA TODOS OS DATAFRAMES DE BO'S:

    df_dicio = {}

    directory = os.listdir(path)

    for file in directory:
        if file != 'auxiliar':
            if file.startswith('RDO'):
                nome_filev = file
                df_v = pd.read_csv(path + '/' + nome_filev)
                nome_key = file[:-4]
                df_dicio[nome_key] = df_v


            elif file[:-4][-1] == '1':        #para os files que representam 1o semestres:
                for file2 in directory:
                    if (file != file2) and (file[:-5] == file2[:-5]):       #encontrando o 2o semestre do ano em questao:
                        nome_file1 = file
                        nome_file2 = file2
                        df1 = pd.read_csv(path + '/' + nome_file1)
                        df2 = pd.read_csv(path + '/' + nome_file2)

                        nome_key = file[:-6]

                        df_dicio[nome_key] = pd.concat([df1 ,df2], ignore_index=True)   #juntando os dois semestres.

            elif (file[:-4][-1] != '1') and (file[:-4][-1] != '2'):       #para os files que nao sao subdivididos em 2 semestres:
                nome_file = file
                df = pd.read_csv(path + '/' + nome_file)
                nome_key = file[:-4]
                df_dicio[nome_key] = df

    return df_dicio
