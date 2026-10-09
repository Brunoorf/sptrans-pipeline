from datetime import datetime
import requests
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"

@dag(
    dag_id="sptrans_dim_corredores",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["sptrans", "dimensao"]
)
def sptrans_dim_corredores():
    @task
    def extrair_corredores() -> list[dict]:
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

        resp = sessao.get(f"{BASE_URL}/Corredor", timeout=30)
        resp.raise_for_status()
        return resp.json()

    @task
    def carregar_corredores(corredores: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id="postgres_sptrans")
        sql = """
            INSERT INTO sptrans_raw.raw_corredores (codigo_corredor, nome_corredor)
            VALUES (%s, %s)
            ON CONFLICT (codigo_corredor) DO UPDATE
            SET nome_corredor = EXCLUDED.nome_corredor,
                updated_at = now();
        """
        registros = [(c["cc"], c["nc"]) for c in corredores]

        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, registros)
        conn.commit()
        conn.close()
        return len(registros)

    carregar_corredores(extrair_corredores())

sptrans_dim_corredores()