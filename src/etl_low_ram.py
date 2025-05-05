import os
import pandas as pd
from pathlib import Path
import gc
from utils import tratamento
from db import get_engine

def extract(path_raw: str, path_processed: str):
    os.makedirs(path_processed, exist_ok=True)
    arquivos = os.listdir(path_raw)

    colunas_padrao = pd.read_csv(os.path.join(path_raw, "BO_2016.csv"), low_memory=False).columns

    for file in arquivos:
        if not file.endswith(".csv") or file.startswith("auxiliar"):
            continue

        file_path = os.path.join(path_raw, file)
        nome_base = file.replace(".csv", "")

        print(f"Processando {file}...")

        df = pd.read_csv(file_path, low_memory=False)
        df = df.loc[:, df.columns.intersection(colunas_padrao)]
        df = tratamento(df)
        df.to_parquet(os.path.join(path_processed, f"{nome_base}.parquet"), index=False)

        del df
        gc.collect()

    print("Extração e tratamento finalizados.")



def load(path_processed: str, tabela_destino: str = "bo_crimes"):
    from db import get_engine

    engine = get_engine()
    arquivos = sorted(Path(path_processed).glob("*.parquet"))

    # Usa BO_2016 como referência de estrutura
    colunas_padrao = pd.read_parquet(path_processed + "/BO_2016.parquet").columns.tolist()

    primeiro = True
    for f in arquivos:
        print(f"Carregando {f.name} no banco...")

        df = pd.read_parquet(f)
        df = df.reindex(columns=colunas_padrao)

        if primeiro:
            modo = "replace"
            primeiro = False
        else:
            modo = "append"

        df.to_sql(tabela_destino, con=engine, if_exists=modo, index=False)

        del df
        gc.collect()

    print("Carga finalizada.")


if __name__ == "__main__":
    raw_path = "/home/thiago/python-projects/crime-sp/data/raw"
    processed_path = "/home/thiago/python-projects/crime-sp/data/processed"

    extract(raw_path, processed_path)
    load(processed_path, "bo_crimes")



