"""Carrega a amostra em Parquet num banco vazio.

Permite reproduzir o projeto sem credencial da API: depois de criar o schema
com sql/ddl_bronze.sql, este script popula a camada bronze e os models do dbt
podem rodar normalmente.

Uso:
    python scripts/carregar_amostra.py
"""
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

CONEXAO = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
ORIGEM = Path(__file__).resolve().parent.parent / "dados_amostra"

# Ordem importa: dimensões antes do fato, por causa das referências
TABELAS = [
    "raw_corredores",
    "raw_empresas",
    "raw_linhas",
    "raw_paradas",
    "raw_veiculo_empresa",
    "etl_execucao",
    "raw_posicao",
]


def main() -> None:
    engine = create_engine(CONEXAO)

    if not ORIGEM.exists():
        raise SystemExit(f"Pasta nao encontrada: {ORIGEM}")

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS sptrans_raw"))

    for tabela in TABELAS:
        arquivo = ORIGEM / f"{tabela}.parquet"
        if not arquivo.exists():
            print(f"{tabela:<24} arquivo ausente, ignorado")
            continue

        df = pd.read_parquet(arquivo)
        df.to_sql(
            tabela,
            engine,
            schema="sptrans_raw",
            if_exists="append",
            index=False,
            chunksize=10_000,
            method="multi",
        )
        print(f"{tabela:<24} {len(df):>9,} linhas carregadas")

    print("\nPronto. Rode o dbt build para construir staging e marts.")


if __name__ == "__main__":
    main()