"""Exporta uma amostra das tabelas bronze para Parquet.

As dimensões saem inteiras (são pequenas). Da tabela de fato sai apenas um dia,
o suficiente para rodar os models do dbt sem precisar de credencial da API.

Uso:
    python scripts/exportar_amostra.py
"""
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

CONEXAO = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
DIA_AMOSTRA = "2026-09-28"
SAIDA = Path(__file__).resolve().parent.parent / "dados_amostra"

DIMENSOES = [
    "raw_linhas",
    "raw_paradas",
    "raw_empresas",
    "raw_corredores",
    "raw_veiculo_empresa",
    "etl_execucao",
]


def main() -> None:
    engine = create_engine(CONEXAO)
    SAIDA.mkdir(exist_ok=True)

    for tabela in DIMENSOES:
        df = pd.read_sql(f"SELECT * FROM sptrans_raw.{tabela}", engine)
        destino = SAIDA / f"{tabela}.parquet"
        df.to_parquet(destino, index=False, compression="snappy")
        print(f"{tabela:<24} {len(df):>9,} linhas")

    sql_posicao = """
        SELECT *
        FROM sptrans_raw.raw_posicao
        WHERE (ta_utc AT TIME ZONE 'America/Sao_Paulo')::date = %(dia)s
    """
    df = pd.read_sql(sql_posicao, engine, params={"dia": DIA_AMOSTRA})
    destino = SAIDA / "raw_posicao.parquet"
    df.to_parquet(destino, index=False, compression="snappy")
    print(f"{'raw_posicao':<24} {len(df):>9,} linhas  ({DIA_AMOSTRA})")

    total_mb = sum(p.stat().st_size for p in SAIDA.glob("*.parquet")) / 1024**2
    print(f"\nTotal em disco: {total_mb:.1f} MB")
    if total_mb > 50:
        print("Acima de 50 MB — considere reduzir a janela de DIA_AMOSTRA.")


if __name__ == "__main__":
    main()