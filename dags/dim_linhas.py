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
    dag_id="sptrans_dim_linhas",
    start_date=datetime(2026, 9, 1),
    schedule="0 */4 * * *",
    catchup=False,
    tags=["sptrans", "dimensao"],
)
def sptrans_dim_linhas():

    @task
    def extrair_linhas() -> list[dict]:
        sessao = autenticar()
        resp = sessao.get(f"{BASE_URL}/Posicao", timeout=60)
        resp.raise_for_status()

        linhas = {}
        for grupo in resp.json()["l"]:
            linhas[grupo["cl"]] = {
                "cl": grupo["cl"],
                "letreiro": grupo["c"],
                "sentido": grupo["sl"],
                "terminal_principal": grupo["lt0"],
                "terminal_secundario": grupo["lt1"],
            }
        return list(linhas.values())

    @task
    def carregar_linhas(linhas: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id="postgres_sptrans")
        sql = """
            INSERT INTO sptrans_raw.raw_linhas
                (codigo_linha, letreiro, sentido,
                 terminal_principal, terminal_secundario)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (codigo_linha) DO UPDATE
            SET letreiro            = EXCLUDED.letreiro,
                sentido             = EXCLUDED.sentido,
                terminal_principal  = EXCLUDED.terminal_principal,
                terminal_secundario = EXCLUDED.terminal_secundario,
                updated_at          = now();
        """
        registros = [
            (
                l["cl"],
                l["letreiro"],
                l["sentido"],
                l["terminal_principal"],
                l["terminal_secundario"],
            )
            for l in linhas
        ]

        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, registros)
        conn.commit()
        conn.close()
        return len(registros)

    carregar_linhas(extrair_linhas())


sptrans_dim_linhas()