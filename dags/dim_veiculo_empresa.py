from datetime import datetime, timedelta

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
    dag_id="sptrans_dim_veiculo_empresa",
    start_date=datetime(2026, 9, 1),
    schedule="0 7,11,15,19 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["sptrans", "dimensao"],
)
def sptrans_dim_veiculo_empresa():

    @task
    def mapear_veiculos() -> dict:
        hook = PostgresHook(postgres_conn_id="postgres_sptrans")
        empresas = [
            r[0] for r in hook.get_records(
                "SELECT codigo_empresa FROM sptrans_raw.raw_empresas "
                "ORDER BY codigo_empresa"
            )
        ]

        sessao = autenticar()
        associacoes = {}
        falhas = []
        for codigo in empresas:
            try:
                resp = sessao.get(
                    f"{BASE_URL}/Posicao/Garagem",
                    params={"codigoEmpresa": codigo},
                    timeout=60,
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception as e:
                falhas.append((codigo, str(e)[:120]))
                continue

            if not payload or not payload.get("l"):
                continue

            for grupo in payload["l"]:
                for v in grupo["vs"]:
                    associacoes[str(v["p"])] = codigo

        if len(falhas) > len(empresas) / 2:
            raise RuntimeError(
                f"Falha em {len(falhas)} de {len(empresas)} empresas"
            )

        registros = [(prefixo, cod) for prefixo, cod in associacoes.items()]

        sql = """
            INSERT INTO sptrans_raw.raw_veiculo_empresa
                (prefixo_veiculo, codigo_empresa)
            VALUES (%s, %s)
            ON CONFLICT (prefixo_veiculo) DO UPDATE
            SET codigo_empresa = EXCLUDED.codigo_empresa,
                ultimo_visto   = now(),
                vezes_visto    = sptrans_raw.raw_veiculo_empresa.vezes_visto + 1;
        """

        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, registros)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM sptrans_raw.raw_veiculo_empresa")
            total = cur.fetchone()[0]
        conn.close()

        print(f"Empresas consultadas: {len(empresas)}")
        print(f"Veiculos nesta coleta: {len(registros)}")
        print(f"Total acumulado:       {total}")
        print(f"Empresas com falha:    {len(falhas)}")
        for codigo, erro in falhas:
            print(f"  empresa {codigo}: {erro}")

        return {"nesta_coleta": len(registros), "total": total}

    mapear_veiculos()


sptrans_dim_veiculo_empresa()