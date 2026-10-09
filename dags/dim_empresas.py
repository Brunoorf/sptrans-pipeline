from datetime import datetime

import requests
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"


def autenticar() -> requests.Session:
    sessao = requests.Session()
    token = Variable.get("sptrans_api_token")
    auth = sessao.post(
        f"{BASE_URL}/Login/Autenticar",
        params={"token": token},
        data="",
        timeout=30,
    )
    auth.raise_for_status()
    if auth.text.strip().lower() != "true":
        raise ValueError("Autenticacao na API Olho Vivo falhou")
    return sessao


@dag(
    dag_id="sptrans_dim_empresas",
    start_date=datetime(2026, 9, 1),
    schedule="@weekly",
    catchup=False,
    tags=["sptrans", "dimensao"],
)
def sptrans_dim_empresas():

    @task
    def extrair_empresas() -> list[dict]:
        sessao = autenticar()
        resp = sessao.get(f"{BASE_URL}/Empresa", timeout=30)
        resp.raise_for_status()

        empresas = {}
        for area in resp.json()["e"]:
            for emp in area["e"]:
                empresas[emp["c"]] = {
                    "codigo_empresa": emp["c"],
                    "nome_empresa": emp["n"],
                    "codigo_area": area["a"],
                }
        return list(empresas.values())

    @task
    def carregar_empresas(empresas: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id="postgres_sptrans")
        sql = """
            INSERT INTO sptrans_raw.raw_empresas
                (codigo_empresa, nome_empresa, codigo_area)
            VALUES (%s, %s, %s)
            ON CONFLICT (codigo_empresa) DO UPDATE
            SET nome_empresa = EXCLUDED.nome_empresa,
                codigo_area  = EXCLUDED.codigo_area,
                updated_at   = now();
        """
        registros = [
            (e["codigo_empresa"], e["nome_empresa"], e["codigo_area"])
            for e in empresas
        ]

        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, registros)
        conn.commit()
        conn.close()
        return len(registros)

    carregar_empresas(extrair_empresas())


sptrans_dim_empresas()