import pandas as pd
import os
import glob

# Uma função de extract que le e consolida json.

def extrair_dados(path: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

# Uma função que transforma.

def calculo_total_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["total"] = df["Quantidade"] * df["Venda"]
    return df

# Uma função que da load em csv ou parquet.

def carregar_dados(df: pd.DataFrame, formato_saida: list): 
    if formato_saida == 'csv':
        df.to_csv("dados.csv", index=False)
    if formato_saida == 'parquet':
        df.to_parquet("dados.parquet", index=False)

# Função final para ser chamada dentro do pipeline.

def executar_pipeline(pasta: str, formato_saida: str):
    dados = extrair_dados(path=pasta)
    dados_com_total = calculo_total_vendas(dados)
    carregar_dados(dados_com_total, formato_saida)
