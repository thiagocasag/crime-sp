import polars as pl
import os
from pathlib import Path
import gc
from utils import tratamento_polars  # nova função adaptada
import psutil

def memoria_usada_em_gb():
    return round(psutil.Process(os.getpid()).memory_info().rss / (1024**3), 2)

def extract_polars(path_raw: str) -> Path:
    arquivos = os.listdir(path_raw)
    processed_dir = Path(path_raw).parent / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    arquivos_processados = set()

    # usa BO_2016 como referência de colunas
    path_ref = os.path.join(path_raw, "BO_2016.csv")
    df_ref = pl.read_csv(
                        path_ref,
                        infer_schema_length=10000,
                        null_values=["NULL"],
                        ignore_errors=True
                        )

    df_ref = tratamento_polars(df_ref)
    colunas_padrao = df_ref.columns

    df_ref.write_parquet(processed_dir / "BO_2016.parquet")
    del df_ref
    gc.collect()
    print(f"[INFO] BO_2016 processado. RAM usada: {memoria_usada_em_gb()} GB")

    for file in arquivos:
        if file in {'auxiliar', 'BO_2016.csv'} or file in arquivos_processados:
            continue

        caminho = os.path.join(path_raw, file)
        nome_base = Path(file).stem
        parquet_path = processed_dir / f"{nome_base}.parquet"

        try:
            if file.endswith("_1.csv"):
                file2 = file.replace("_1.csv", "_2.csv")
                if file2 in arquivos:
                    df1 = pl.read_csv(os.path.join(path_raw, file))
                    df2 = pl.read_csv(os.path.join(path_raw, file2))
                    df = pl.concat([df1, df2])
                    arquivos_processados.update([file, file2])
                else:
                    print(f"[AVISO] Semestre 2 de {file} não encontrado.")
                    continue
            else:
                df = pl.read_csv(caminho)

            df = tratamento_polars(df)
            df = df.select([col for col in colunas_padrao if col in df.columns])
            df.write_parquet(parquet_path)

            print(f"[OK] {file} tratado. RAM usada: {memoria_usada_em_gb()} GB")

            del df
            gc.collect()

        except Exception as e:
            print(f"[ERRO] ao processar {file}: {e}")

    return processed_dir


def transform_polars(processed_dir: Path) -> pl.DataFrame:
    arquivos = list(processed_dir.glob("*.parquet"))
    dfs = [pl.read_parquet(f) for f in arquivos]
    df_final = pl.concat(dfs, how="vertical_relaxed")
    print(f"[INFO] Transform finalizado. RAM usada: {memoria_usada_em_gb()} GB")
    return df_final


def load_polars(df: pl.DataFrame, table_name: str = "bo_crimes"):
    import sqlalchemy
    from src.db import get_engine
    engine = get_engine()
    df_pandas = df.to_pandas()
    df_pandas.to_sql(table_name, con=engine, if_exists="replace", index=False)
    print(f"[INFO] Tabela '{table_name}' carregada no banco.")


if __name__ == "__main__":
    raw_path = "/home/thiago/python-projects/crime-sp/data/raw"
    processed = extract_polars(raw_path)
    df_final = transform_polars(processed)
    load_polars(df_final)
