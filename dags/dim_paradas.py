from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_URL = "http://api.olhovivo.sptrans.com.br/v2.1"
TAMANHO_LOTE = 50

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
    dag_id="sptrans_dim_paradas",
    start_date=datetime(2026, 9, 1),
    schedule="@weekly",
    catchup=False,
    max_active_tasks=4,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["sptrans", "dimensao"],
)

def sptrans_dim_paradas():

    @task
    def montar_lotes() -> list[list[int]]:
        hook = PostgresHook(postgres_conn_id="postgres_sptrans")
        registros = hook.get_records(
            "SELECT codigo_linha FROM sptrans_raw.raw_linhas ORDER BY codigo_linha"
        )
        codigos = [r[0] for r in registros]
        return [
            codigos[i : i + TAMANHO_LOTE]
            for i in range(0, len(codigos), TAMANHO_LOTE)
        ]

    @task
    def processar_lote(codigos: list[int]) -> dict:
        sessao = autenticar()
        paradas = {}
        linhas_com_parada = 0

        for codigo in codigos:
            resp = sessao.get(
                f"{BASE_URL}/Parada/BuscarParadasPorLinha",
                params={"codigoLinha": codigo},
                timeout=30,
            )
            resp.raise_for_status()
            retorno = resp.json()

            if retorno:
                linhas_com_parada += 1

            for p in retorno:
                chave = (str(p["cp"]), codigo)
                paradas[chave] = (
                    str(p["cp"]),
                    codigo,
                    p["np"],
                    p["ed"],
                    p["py"],
                    p["px"],
                )

        registros = list(paradas.values())

        if registros:
            sql = """
                INSERT INTO sptrans_raw.raw_paradas
                    (codigo_parada, codigo_linha, nome_parada,
                     endereco, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (codigo_parada, codigo_linha) DO UPDATE
                SET nome_parada = EXCLUDED.nome_parada,
                    endereco    = EXCLUDED.endereco,
                    latitude    = EXCLUDED.latitude,
                    longitude   = EXCLUDED.longitude,
                    updated_at  = now();
            """
            hook = PostgresHook(postgres_conn_id="postgres_sptrans")
            conn = hook.get_conn()
            with conn.cursor() as cur:
                cur.executemany(sql, registros)
            conn.commit()
            conn.close()

        return {
            "linhas_consultadas": len(codigos),
            "linhas_com_parada": linhas_com_parada,
            "paradas_gravadas": len(registros),
        }

    @task
    def consolidar(resultados: list[dict]) -> None:
        consultadas = sum(r["linhas_consultadas"] for r in resultados)
        com_parada = sum(r["linhas_com_parada"] for r in resultados)
        gravadas = sum(r["paradas_gravadas"] for r in resultados)
        cobertura = com_parada / consultadas * 100 if consultadas else 0

        print(f"Linhas consultadas:   {consultadas}")
        print(f"Linhas com parada:    {com_parada}")
        print(f"Cobertura:            {cobertura:.1f}%")
        print(f"Pares parada-linha:   {gravadas}")

    lotes = montar_lotes()
    resultados = processar_lote.expand(codigos=lotes)
    consolidar(resultados)


sptrans_dim_paradas()